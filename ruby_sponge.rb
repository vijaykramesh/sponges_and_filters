
require 'benchmark'
require 'json'

require './etc/s3_config'

module SpongesAndFilters

  class RubySponge

    def self.soak
      Benchmark.bm do |s|
        s.report("1 file")    { JSON.pretty_generate(new(1).quantile_stats)  }
        s.report("2 files")   { JSON.pretty_generate(new(2).quantile_stats)  }
        s.report("10 file")   { JSON.pretty_generate(new(10).quantile_stats) }
        s.report("100 file")  { JSON.pretty_generate(new(10).quantile_stats) }
        s.report("1000 file") { JSON.pretty_generate(new(10).quantile_stats) }
      end
    end

    attr_accessor :max_files, :number_of_quantiles
    def initialize(max_files, number_of_quantiles = 5)
      @max_files = max_files
      @number_of_quantiles = number_of_quantiles
    end

    def quantile_stats
      Hash[quantile_users.each_with_index.map {|u,i|
        [i, u.count]
      }]
    end

    private

    # returns an array of chomped lines
    def each_row
      @each_row ||= begin
        seen = 1 # can't use .take(max_files) with aws-sdk as it forces all file objects to get statted
        s3_bucket.objects.with_prefix("signature_counts/part-").map {|s3_object|
          next if seen > max_files
          seen += 1
          gunzip(s3_object.read).split("\n")[1..-1] # ignore headers
        }.compact.flatten
      end
    end

    # build a hash of uid => signature_count
    def signatures
      @signatures ||= begin
        Hash[each_row.map {|l|
            uid, signature_count = l.chomp.split("\t")
            [uid.to_i, signature_count.to_i]
          }]
      end
    end

    def quantile_users
      quantiles = [[]]
      current_quantile = 0
      signatures.sort_by {|k,v| v }.each_slice(users_per_quantile) {|slice|
        slice.each {|uid, signature_count|
          quantiles[current_quantile] << uid
          if quantiles[current_quantile].size > users_per_quantile
            current_quantile += 1 
            quantiles << []
          end
        }
      }
      quantiles
    end

    def s3_bucket
      @s3_bucket ||= AWS::S3.new.buckets[S3_BUCKET_NAME]
    end

    def gunzip(string)
      Zlib::GzipReader.new(StringIO.new(string)).read
    end

    def users_per_quantile
      @users_per_quantile ||= signatures.count / number_of_quantiles
    end
  end
end