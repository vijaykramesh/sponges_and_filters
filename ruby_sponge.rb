
require 'benchmark'
require 'json'

require './etc/s3_config'

module SpongesAndFilters

  class RubySponge

    def self.soak(source_name = "signature_counts", max_files = nil)

      if max_files
        sponge = new(source_name, max_files)
        sponge.write_quantiled
        puts JSON.pretty_generate(sponge.quantile_stats)
      else
        Benchmark.bm do |s|
          s.report("1 file") {
            sponge = new(source_name, 1)
            puts JSON.pretty_generate(sponge.quantile_stats)
          }

          s.report("2 files")   {
            stats = new(source_name, 2).quantile_stats
            puts JSON.pretty_generate(stats)
          }
          s.report("10 files")   {
            stats = new(source_name, 10).quantile_stats
            puts JSON.pretty_generate(stats)
          }
          s.report("100 files")   {
           stats = new(source_name, 100).quantile_stats
            puts JSON.pretty_generate(stats)
          }
        end
      end
    end

    attr_accessor :max_files, :number_of_quantiles, :source_name
    def initialize(source_name, max_files, number_of_quantiles = 5)
      @source_name = source_name
      @max_files = max_files
      @number_of_quantiles = number_of_quantiles
    end

    def write_quantiled
      s3_bucket.objects["ruby_sponge/#{source_name}/#{max_files}/quantiled"].write(
        quantiled_users.each_with_index.map {|users, quantile_index|
          users.map {|uid| [uid, quantile_index].join("\t") }
        }.flatten.join("\n")
      )
    end

    def quantile_stats
      Hash[quantiled_users.each_with_index.map {|u,i|
        [i, u.count]
      }].merge({"total" => quantiled_users.flatten.count})
    end

    private

    # returns an array of chomped lines
    def each_row
      @each_row ||= begin
        @seen = 1 # can't use .take(max_files) with aws-sdk as it forces all file objects to get statted
        rows = get_rows
        while @seen < max_files
          rows += get_rows
        end
        rows
      end
    end

    def get_rows
      s3_bucket.objects.with_prefix("#{source_name}/part-").map {|s3_object|
        next if @seen > max_files
        @seen += 1
        gunzip(s3_object.read).split("\n")[1..-1] # ignore headers
      }.compact.flatten
    end

    # build a hash of uid => raw_value
    def hash
      @hash ||= begin
        Hash[each_row.map {|l|
            uid, val = l.chomp.split("\t")
            [uid.to_i, val.to_f]
          }]
      end
    end

    def quantiled_users
      @quantiled_users ||= begin
        [].tap {|quantiles|
          quantiles << []
          current_quantile = 0
          hash.sort_by {|k,v| v }.each_slice(users_per_quantile) {|slice|
            slice.each {|uid, val|
              quantiles[current_quantile] << uid
              if quantiles[current_quantile].size > users_per_quantile
                current_quantile += 1
                quantiles << []
              end
            }
          }
        }
      end
    end

    def s3_bucket
      @s3_bucket ||= AWS::S3.new.buckets[S3_BUCKET_NAME]
    end

    def gunzip(string)
      Zlib::GzipReader.new(StringIO.new(string)).read
    end

    def users_per_quantile
      @users_per_quantile ||= hash.count / number_of_quantiles
    end
  end
end