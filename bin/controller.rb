#!/usr/bin/env ruby


class Controller

  S3_BUCKET = "change-blog-datascience"

  AWS_REGION = "us-west-2"

  PROJECT_ROOT = File.expand_path(File.dirname(__FILE__) + "/..")

  class Quantile < Controller
    def num_instances ; 10 ; end
    def which_round ; "quantile" ; end
  end

  def self.run(which_round, which_source, num_files)
    begin
      eval(which_round.capitalize).new(which_source, num_files).run
    rescue Exception => e
      puts e
      raise ArgumentError, "Usage: $0 (quantile) which_source num_files?"
    end
  end

  def squeeze_command(cmd)
    cmd.gsub(/\s+/, ' ').strip
  end

  def shell_out(cmd)
    system(squeeze_command(cmd))
  end

  attr_accessor :which_source, :num_files

  def initialize(which_source, num_files)
    @which_source = which_source
    @num_files = num_files
  end

  def job_name
    @job_name ||= `grep -w jobName #{PROJECT_ROOT}/etc/emr.properties | cut -d' ' -f2`.chomp
  end

  def s3_project_root # caller must preface with URI scheme
    "//#{S3_BUCKET}/#{job_name}"
  end

  def local_jar_file
    "#{PROJECT_ROOT}/build/libs/sponges_and_filters.jar"
  end

  def emr_name
    "#{`git config user.name`.chomp}: #{job_name} #{which_round}"
  end

  def s3_jar_path
    md5 = `md5 -q #{local_jar_file}`.chomp
    "#{s3_project_root}/transform_#{md5[0..7]}.jar"
  end

  def s3put
    "s3cmd put #{local_jar_file} s3:#{s3_jar_path}"
  end

  # TODO replace this with a call to AWS::EMR
  # that way we can easily get the master node ip
  # and go ahead and open an SSH tunnel so we can
  # view the hadoop admin console
  def emr_command
    <<-SH
      bundle exec elastic-mapreduce \
        -c #{PROJECT_ROOT}/etc/credentials.json \
        --create \
        --region "#{AWS_REGION}" \
        --name "#{emr_name}" \
        --master-instance-type m1.large \
        --slave-instance-type  c1.xlarge \
        --num-instances "#{num_instances}" \
        --jar "s3n:#{s3_jar_path}" \
        --args emr,"#{which_round}", "#{which_source}", "#{num_files}" \
        --log-uri "s3n:#{s3_project_root}/logs" \
        --visible-to-all-users
    SH
  end

  # Build and upload...

  def run
    # dynamic features need to write their inputs if possible
    # TODO check if this needs to be done instead of just doing it


    raise "Could not build JAR!" unless shell_out("gradle jar")

    # FIXME: We get the same filename, thus the same MD5
    # hash, but it still reups, which seems to indicate
    # that S3 thinks we have different MD5s...
    puts 'Uploading JAR file to S3...'
    if `s3cmd ls s3:#{s3_jar_path}`.empty?
      raise "Could not upload JAR!" unless shell_out(s3put)
    else
      puts "JAR with same MD5 already exists at s3:#{s3_jar_path}..."
    end

    # Create job...
    raise "Jobflow creation failed!" unless shell_out(emr_command)

    Kernel.exec("open https://console.aws.amazon.com/elasticmapreduce/home?region=#{AWS_REGION}")
  end
end

Controller.run(*ARGV) if $0 == __FILE__
