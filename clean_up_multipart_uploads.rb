require 'aws-sdk-v1' 
require 'nokogiri' 
require 'yaml'
require 'date'
require 'parallel'

# 
class CleanupMultipartUploads
   
    def initialize  
        config = YAML.load_file("config.yml")
        @src_client = AWS::S3::Client.new( 
          :access_key_id => config["mup"]["access_key"],
          :secret_access_key => config["mup"]["secret_key"],
          :s3_endpoint => config["mup"]["endpoint"]
        )
        @backetlist = config["mup"]["bucketlist"]
        @delete_period = DateTime.now - config["mup"]["retention_period"]
    end

    def clean_up
        @backetlist.each do |bucket|
            puts "#{bucket} -------------"
            bucket_clean_up bucket
        end
    end

private
    def bucket_clean_up(bucket, marker = "")
        multipart_list = list_multipart_uploads(bucket, marker)
        multipart_list[:uploads].each do |multipart|
            puts multipart if delete_it? multipart[:initiated]
            abort(bucket,multipart) if delete_it? multipart[:initiated]
        end
        bucket_clean_up(bucket, multipart_list[:next_key_marker]) unless no_next_marker? multipart_list
    end

    def list_multipart_uploads(bucket, marker)
        @src_client.list_multipart_uploads(
            :bucket_name => bucket ,
            :max_keys => 10000,
            :key_marker => marker
        )
    end

    def abort(bucket,multipart)
        puts "  Start delete #{multipart[:key]}"
        begin
          @src_client.abort_multipart_upload(
             :bucket_name => bucket,
             :key => multipart[:key],
             :upload_id => multipart[:upload_id]
          )
        puts "  End delete #{multipart[:key]}"
        rescue => e
           p e.class
           p e.message
        end
    end

    def delete_it?(datetime)
        datetime <= @delete_period
    end  

    def no_next_marker?(multipart_response)
        multipart_response[:next_key_marker].nil?
    end

end 

ms = CleanupMultipartUploads.new
ms.clean_up


