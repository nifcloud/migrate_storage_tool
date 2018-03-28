require 'aws-sdk-v1' 
require 'nokogiri' 
require 'parallel' 
require 'yaml'
require 'stringio'
require 'logger'

require 'objspace'


class MigrateStorage
    
    def initialize  
        config = YAML.load_file("config.yml")
        @target_path = config["src"]["targetpath"]
        @src_client = AWS::S3::Client.new( 
          :access_key_id => config["src"]["access_key"],
          :secret_access_key => config["src"]["secret_key"],
          :s3_endpoint => config["src"]["endpoint"] 
        )
        @src_bucket_name = config["src"]["bucketname"]
        
        @dst_client = AWS::S3::Client.new( 
          :access_key_id => config["dst"]["access_key"],
          :secret_access_key => config["dst"]["secret_key"],
          :s3_endpoint => config["dst"]["endpoint"] 
        )
        @dst_bucket_name = config["dst"]["bucketname"]
 
        @logger = Logger.new("log/migrate_obj_#{@src_bucket_name}.log")
        @retry_max = 10 #times
        @sleep_time = 3 #sec
        @move_headers = config["src"]["headers"]
        @cpu_numbers = config["multiple"]["cpu_number"]
        @thread_numbers = config["multiple"]["thread_number"]

    end
   
    def migrage
        @logger.info "Gettign list start"
        object_list = create_obj_list
        @logger.info " Object list size:#{object_list.size}"
        @logger.info "Gettign list finished"
        @logger.info "Migrate data start"
        parallel_process_migrate(object_list.shuffle!, @cpu_numbers)
        @logger.info "Migrate data finished"
        exit 0
    end

    def migrate_from_file(file)
        thread_id =  create_thread_id 7
        object_list = Array.new
        @logger.info "Gettign list from file start"
        File.foreach(file) do |line|
           src = eval(line)
           src_obj = AWS::Core::Data.new({
              :key => src[:key],
              :size => src[:size], 
              :etag => src[:etag]}
           )
           object_list.push src_obj
        end
        @logger.info " Object list size:#{object_list.size}"
        @logger.info "Gettign list finished"
        @logger.info "Migrate data start"
        parallel_thread_migrate(object_list , thread_id , @thread_numbers)
        @logger.info "Migrate data finished"
    end


private
    def create_obj_list(marker = "",list = nil) 
        list ||= Array.new 
        resp = src_objects(marker) 
        resp.contents.each do |obj|
          list.push obj
        end
        create_obj_list(resp.next_marker,list) if next_page? resp
        list
    end

    def src_objects(marker = "")
        param = {
          :bucket_name => @src_bucket_name,
          :marker => marker,
          :delimiter => '*'
        }
        param[:prefix] = @target_path unless @target_path.nil?
        @src_client.list_objects param
    end
    
    def get_object(key)
        ret_cnt = 0
        begin
           obj = @src_client.get_object( 
             :bucket_name => @src_bucket_name,
             :key => key
           )
        rescue Timeout::Error => ex
           @logger.warn "Get Error key:#{key} masssage:#{ex.message}"
           @logger.warn ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        rescue => ex
           @logger.warn "Get Error key:#{key} masssage:#{ex.message}"
           @logger.warn ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        end
        obj
    end
    
    def get_src_acl(key)
        ret_cnt = 0
        begin
           acl = @src_client.get_object_acl( 
             :bucket_name => @src_bucket_name,
             :key => key
           )
        rescue Timeout::Error => ex
           @logger.warn "Get Error acl of key:#{key} masssage:#{ex.message}"
           @logger.warn ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        rescue => ex
           @logger.warn "Get Error acl of key:#{key} masssage:#{ex.message}"
           @logger.warn ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        end
        acl
    end
    
    def put_object(key,data,acl)
        ret_cnt = 0
        begin
           put_param = create_put_param(key,data,acl)
           obj = @dst_client.put_object(put_param )
        rescue => ex
           @logger.warn "Get Error key:#{key} masssage:#{ex.message}"
           @logger.warn ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        end
        obj
    end

    def create_put_param(key,data,acl)
       param = {:bucket_name => @dst_bucket_name, :key => key, :acl => acl,:data => data.http_response.body}
       meta_hash = create_add_params(data.http_response.headers)
       param.merge! meta_hash
       param
    end

    def create_add_params(src_data_headers)
       create_meta_hash(src_data_headers).merge create_another_headers(src_data_headers) 
    end

    def create_meta_hash(src_data_headers)
       get_meta_hash = src_data_headers.select{|k, v| k.match(/^x-amz-meta/) }
       meta_data_hash = Hash.new
       return meta_data_hash if get_meta_hash.size == 0
       get_meta_hash.each do  |k, v| 
           metakey =  k.to_s.gsub(/^x-amz-meta-/,"")
           metavalue = v.to_s.gsub(/^\[\"/,"").gsub(/\"\]$/,"")
           meta_data_hash[metakey] = metavalue 
       end
       {:metadata => meta_data_hash}
    end
    
    def create_another_headers(src_data_headers)
       another_hash = Hash.new
       begin
          @move_headers.each do |header,sym|
             header.each do | k,v |
                another_hash[v] = src_data_headers[k].to_s.gsub(/^\[\"/,"").gsub(/\"\]$/,"") unless src_data_headers[k].size == 0
             end
          end
       rescue => ex
           @logger.warn "Create Header WARN of key:#{@move_headers} masssage:#{ex.message}"
           @logger.warn ex
       end
       another_hash 
    end
   
    def next_page?(resp)
        begin
           resp.next_marker
           true
        rescue => e
           @logger.info "#{e.class.to_s}:  #{e.message}" unless e.message =~ /undefined method `next_marker' for/ 
           false          
        end
    end


    def parallel_process_migrate(object_list, parallel_num)
        begin 
           Parallel.each(object_list.each_slice(set_array_slice_num(object_list, parallel_num)), in_processes: parallel_num) do |slice_list|
              thread_id = create_thread_id 6
              parallel_thread_migrate(slice_list , thread_id , 1)
           end
        rescue => ex
           @logger.error "Get Error in Parallel process #{ex.message}"
           @logger.error ex
        end
    end


    def parallel_thread_migrate(object_list, thread_id , threads)
        ret_cnt =  0
        obj_cnt = 0
        puts_object_list_to_tmp_file(object_list,thread_id)
        begin 
           Parallel.each(object_list, in_threads: threads) do |src_obj|
              dst_obj = put_to_dst_storage(src_obj,thread_id)
              obj_cnt += 1
              dst_obj = nil
              src_obj = nil
              GC.start if (obj_cnt % 100 == 0)
           end
        rescue Parallel::DeadWorker => ex
           @logger.info "Get Error #{ex.message}"
           @logger.info ex
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        rescue => ex
           @logger.error "Get Error in #{thread_id} #{ex.message}"
           @logger.error ex
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
        end
    end

    def put_to_dst_storage(src_obj,thread_id)
       ret_cnt =  0
       begin 
           data = get_object(src_obj.key)
           
           #acl =  get_src_acl(src_obj.key)
           acl =  :private
           #acl =  :public_read
           dst_data = put_object(src_obj.key,data,acl)
           @logger.info "thread:#{thread_id} key: #{src_obj.key} "
           data = nil
           raise unless  check_put_object_result(src_obj,dst_data)
           @logger.info "thread:#{thread_id} key: #{src_obj.key} "
       rescue
           ret_cnt += 1
           sleep @sleep_time
           retry if ret_cnt < @retry_max 
       end
       dst_data
    end

    def check_put_object_result(src_obj,dst_obj)
       begin 
           if src_obj.etag != dst_obj.etag
               @logger.info "#{src_obj.key} s Etag is changed !!! src_etag: #{src_obj.etag}  dst_etag: #{dst_obj.etag}"
               return false
           end
       rescue => ex
           @logger.warn "Get Error #{src_obj.key}s Etag check is error masssage:#{ex.message}"
       end
       true
    end
    
    def set_array_slice_num(object_list, parallel_num)
        return 1 unless object_list.size > parallel_num
        object_list.size / parallel_num
    end

    def create_thread_id(size)
      ((0..9).to_a + ("a".."z").to_a + ("A".."Z").to_a).sample(size).join
    end

    def puts_object_list_to_tmp_file(list,thread_id)
      File.open("tmp/object_list_#{thread_id}.list","w") do |f|
         list.each { |item| 
            objlog = "{:key=>\'#{item.key}\',  :size=>\'#{item.size}\', :etag=>\'#{item.etag}\'}"
            f.puts(objlog)
         }
      end
    end

end 
