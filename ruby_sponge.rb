
require 'benchmark'
require 'json'

require './etc/s3_config'
require './lib/memory_analyzer'

module SpongesAndFilters

  class RubySponge

    def self.soak(max_files = nil)

      if max_files
        sponge = new(max_files)
        puts JSON.pretty_generate(sponge.quantile_stats)
        puts MemoryAnalyzer.new(sponge).calculate_size.to_f / 1024.0
      else
        Benchmark.bm do |s|
          s.report("1 file") {
            sponge = new(1)
            puts JSON.pretty_generate(sponge.quantile_stats)
            puts MemoryAnalyzer.new(sponge).calculate_size.to_f / 1024.0
          }

          s.report("2 files")   {
            stats = new(2).quantile_stats
            size = MemoryAnalyzer.new(stats).calculate_size
            puts JSON.pretty_generate(stats)
            puts size.to_f/ 1024.0
          }
          s.report("10 files")   {
            stats = new(10).quantile_stats
            size = MemoryAnalyzer.new(stats).calculate_size
            puts JSON.pretty_generate(stats)
            puts size.to_f/ 1024.0
          }
          s.report("100 files")   {
           stats = new(100).quantile_stats
            size = MemoryAnalyzer.new(stats).calculate_size
            puts JSON.pretty_generate(stats)
            puts size.to_f/ 1024.0
          }
          # s.report("1000 files")   {
          #   stats = new(1000).quantile_statss
          #   size = Knj::Memory_analyzer::Object_size_counter.new(stats).calculate_size
          #   puts JSON.pretty_generate(stats)
          #   puts size.to_f/ 1024.0
          # }
        end
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
      }].merge({"total" => quantile_users.flatten.count})
    end

    private

    # returns an array of chomped lines
    def each_row
      @each_row ||= begin
        @seen = 1 # can't use .take(max_files) with aws-sdk as it forces all file objects to get statted
        rows = get_rows()
        while @seen < max_files
          rows += get_rows()
        end
      end
    end

    def get_rows(seen)
      s3_bucket.objects.with_prefix("signature_counts/part-").map {|s3_object|
        next if @seen > max_files
        @seen += 1
        gunzip(s3_object.read).split("\n")[1..-1] # ignore headers
      }.compact.flatten

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