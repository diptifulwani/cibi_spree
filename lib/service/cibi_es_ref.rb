class Cibies_ref
	URL = "localhost:9200"
	# INDEX = "#{User.current_user.account_id}"
	# INDEX = "1"
	BATCH_SIZE = 15
	GET_DATA_KEY_TIMEOUT = 43200  # 30 Minutes
	
	def self.cleanup
		curl = Curl::Easy.new
		curl.close		
		GC.start
	end

	def self.get_unique_keys_cache_key(type_name, field_name, format_as)
		tname = type_name.gsub(/\s+/, '')
		fname = field_name.gsub(/\s+/, '')
		format = format_as.gsub(/\s+/, '')
		return "unique_key_#{tname}_#{fname}_#{format_as}"
	end

	def index_exists
		curl = nil
		url="#{Cibies::URL}/#{Cibies::INDEX}"
		curl = Curl::Easy.http_head(url) do |c|
			c.timeout = 300
		end
		if curl.response_code == 200
			return true
		else
			return false
		end
	end

	def create_index
		curl = nil
		url="#{Cibies::URL}/#{Cibies::INDEX}/"
		curl = Curl::Easy.http_put(url, nil) do |c|
			c.headers['Content-type'] = 'text/json'
			c.timeout = 300
		end
		if curl.response_code == 200
			return true
		else
			return false
		end
	end

	def type_exists(type_name)
		curl = nil
		url="#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}"
		curl = Curl::Easy.http_head(url) do |c|
			c.timeout = 300
		end
		if curl.response_code == 200
			return true
		else
			return false
		end
	end

	def create_type(type_name, fields, config)
		curl = nil
		begin
			unless  self.index_exists
				self.create_index
			end

			table_fields=fields.clone

			table_fields.each do |field|
				if field["fieldType"].start_with?("varchar") || field["fieldType"].start_with?("text")
					field["fieldType"] = "string"
				elsif field["fieldType"].start_with?("time") || field["fieldType"].start_with?("datetime")
					field["fieldType"] = "date"
				elsif field["fieldType"].start_with?("decimal")
					field["fieldType"] = "double"
				elsif field["fieldType"].start_with?("int")
					field["fieldType"] = "long"
				end
			end
			data = '{'
			data += '"'+type_name+'" : {'
			data += '"properties" : {'

			table_fields.each_with_index do |field, i|
				if i != 0
					data += ','
				end
				data += '"'+field["fieldName"]+'" : {'

				if field["fieldType"]=="date"
					data += '"type" : "object",'
					data += '"properties" : {'
					data += '"value" : {"type" : "date", "format" : "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||HH:mm:ss"},'
                    data += '"year" : {"type" : "string"},'
                    data += '"quarter" : {"type" : "string"},'
                    data += '"month" : {"type" : "string"},'
                    data += '"week" : {"type" : "string"},'
                    data += '"day" : {"type" : "string"},'
                    data += '"hour" : {"type" : "string"},'
                    data += '"month year" : {"type" : "string", "index" : "not_analyzed"}'
					data += '}'
				else
					data += '"type" : "'+field["fieldType"]+'"'
					if field["fieldType"]=="string"
						data += ', "fields" : {'
						data += '"raw" : {'
						data += '"type" : "'+field["fieldType"]+'",'
						data += '"index" : "not_analyzed"'
						data += '}'
						data += '}'
					end
				end

				data += '}'
			end

			data += '}'
			data += '}'
			data += '}'

			#  format of a single field

			# "<fieldName>" : {
			# 	"type" : "<fieldType>",
			# 	"fields" : {
			# 		"raw" : {
			# 			"type" : "<fieldType>",
			# 			"index" : "not_analyzed"
			# 		}
			# 	}
			# }


			# $ curl -XPUT 'http://localhost:9200/twitter/tweet/_mapping' -d
			# '{
			# 	"tweet" : {
			# 		"properties" : {
			# 			"message" : {"type" : "string", "store" : true }
			# 		}
			# 	}
			# }'

			# {
			#     "test1" : {
			#         "properties" : {
			#         	"name" : {"type" : "string", "index" : "not_analyzed"},
			#             "order_date" : {
			#                 "type" : "nested",
			#                 "properties" : {
			#                     "value" : {"type" : "date", "format" : "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||HH:mm:ss"},
			#                     "Year" : {"type" : "long"},
			#                     "Quarter" : {"type" : "string"},
			#                     "Month" : {"type" : "long"},
			#                     "Week" : {"type" : "string"},
			#                     "Day" : {"type" : "long"},
			#                     "Hour" : {"type" : "long"},
			#                     "Month Year" : {"type" : "string", "index" : "not_analyzed"}
			#                 }
			#             },
			#             "amount" : {"type" : "double"}
			#         }
			#     }
			# }

			url = "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_mapping"
			curl = Curl::Easy.http_put(url, data) do |c|
				c.headers['Content-type'] = 'text/json'
				c.timeout = 300
			end	
			if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
				curl.close unless curl.nil?
				Cibies::cleanup
				puts "Curl Returned an error response code. #{curl.body_str}"
				Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
				return false
			end
			curl.close unless curl.nil?
			true
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def delete_type(type_name)
		curl = nil
		begin
			if self.type_exists(type_name)
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}"
				curl = Curl::Easy.http_delete(url) do |c|
					c.timeout = 300
				end
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end			
				curl.close unless curl.nil?
			end
			Cibies::cleanup
			true
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def add_data(type_name, table_data, ds_id, unique_keys) 
		url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_bulk"
		failed_curls = [], succeeded_curls = []	
 		$redis.set("rec_count_#{ds_id}", 0)
		begin
			set_size = table_data.length / 10
			set_size = (set_size < 1 ) ? 1 : set_size

			sets = table_data.each_slice(set_size).to_a
			rec_count = 0
			sets.each do |set|
				# m_curl = Curl::Multi.new
				chunks = set.each_slice(Cibies::BATCH_SIZE).to_a
				chunks.each do |data_chunk|
					str=""
					data_chunk.each do |s|
						str+=s
					end
					curl = Curl::Easy.http_post(url, str) do |c|
						c.headers['Content-type'] = 'text/json'
						c.timeout = 300
					end
					if curl.response_code != 200 || JSON.parse(curl.body_str)["errors"] == true
						msg=curl.body_str
						curl.close unless curl.nil?
						puts "Curl Returned an error response code. #{curl.body_str}"
						Rails.logger.error "Curl Returned an error response code"
						return msg
					end
					result = curl.body_str

					curl.close unless curl.nil?								
				end
				rec_count += 1
				puts set_size
					puts rec_count
				upload_percent = ((set_size*rec_count)/(set_size*10).to_f) *100
				$redis.set("rec_count_#{ds_id}", upload_percent.to_i)
					puts '======================================================='
					puts upload_percent.to_i
					puts '======================================================='
			end
			Cibies::cleanup 
			true
		rescue Exception => e
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			puts "#{e.backtrace}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def get_latest_doc_id(type_name)
		# curl -XPOST 'localhost:9200/test/212_contents/_search?pretty' -d '{"query": {"match_all": {}}, "sort" : [ { "_uid" : { "order":"desc" } } ] }'
		
		curl = nil
		begin
			if self.type_exists(type_name)
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_search"
				data = '{
							"query": {"match_all": {}}, 
							"sort" : [ { "_uid" : { "order":"desc" } } ],
							"size" : 1
						}'
				curl = Curl::Easy.http_post(url, data) do |c|
					c.headers['Content-type'] = 'text/json'
					c.timeout = 300
				end
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end
				str = JSON.parse(curl.body_str)
				curl.close unless curl.nil?
				return (str["hits"]["hits"][0]["_id"].to_i + 1)
			else
				return 1
			end			
			Cibies::cleanup			
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def delete_data(type_name, data_content_id)
		curl = nil
			# curl -XDELETE 'localhost:9200/1/230_contents/_query' -d '{"query" : {"term" : { "data_content_id" : "255" } } }'
		begin
			if self.type_exists(type_name)
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_query?q=data_content_id:#{data_content_id}"
				curl = Curl::Easy.http_delete(url) do |c|
					c.timeout = 300
				end
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end			
				curl.close unless curl.nil?
			end
			Cibies::cleanup
			return true
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def get_total(type_name, conditionsMap = nil)
		curl = nil
		# mysql
		# SELECT COUNT(*) FROM TABLE_NAME;
		# elastic search
		# curl -XPOST 'localhost:9200/<index>/type_name/_search?pretty' -d '{"query": {"match_all": {}}, "size":0 }'
		begin
			if self.type_exists(type_name)
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_search"
				data = '{
							"query": {
								"match_all": {}
							}, 
							"size" : 0
						}'
				curl = Curl::Easy.http_post(url, data) do |c|
					c.headers['Content-type'] = 'text/json'
					c.timeout = 300
				end
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end
				str = JSON.parse(curl.body_str)
				curl.close unless curl.nil?
				return str["hits"]["total"]
				# return (str["hits"]["hits"][0]["_id"].to_i + 1)
			else
				return false
			end			
			Cibies::cleanup			
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def get_bookmark_value(type_name, bookmark_key, bookmark_comparison)
		curl = nil
		# mysql
		# SELECT <MIN/MAX>(BOOKMARK_KEY) FROM TABLE_NAME
		# elastic search
		# curl -XPOST 'localhost:9200/<index>/type_name/_search?pretty' -d '{"aggs" :{"bookmark_value" : { "<bookmark_operation>" : { "field" : "<bookmark_key>" } }}, size:0}'
		case bookmark_comparison			
		when ">"
			bookmark_operation = "max"
		when "<"
			bookmark_operation = "min"
		end
		begin		
			if self.type_exists(type_name)		
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_search"
				data = '{'
				data += '"aggs" :{'
				data += '"bookmark_value" : {' 
				data += '"'+bookmark_operation+'" : {'
				if self.get_field_type(type_name, bookmark_key) == "string"
					data += '"field" : "'+bookmark_key+'.raw"'
				else
					data += '"field" : "'+bookmark_key+'"'
				end
				data += '}'
				data += '}'
				data += '},'
				data += 'size : 0'
				data += '}'
				curl = Curl::Easy.http_post(url, data) do |c|
					c.headers['Content-type'] = 'text/json'
					c.timeout = 300
				end	
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end
				str = JSON.parse(curl.body_str)
				curl.close unless curl.nil?
				return str["aggregations"]["bookmark_value"]["value"]
			else
				return false
			end
			Cibies::cleanup
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def get_unique_keys(type_name, field_name, formatAs)

		# mysql
		# SELECT DISTINCT <field_name> FROM <type_name>
		# elastic search
		# curl -XPOST 'localhost:9200/1/222_contents/_search' -d '{"aggs" : { "distinct cities" : { "terms" : {"field" : "City Name", "size":0}}}, "size":0}'

		curl = nil
		begin
			if self.type_exists(type_name)
				if Cibies::caching_enabled?
					cache_key = Cibies::get_unique_keys_cache_key(type_name, field_name, formatAs)
					result = Cibies::cache_get(cache_key)	
					if result
						return result 
					end
				end	
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_search"
				data = '{
							"aggs" : {'
				# if formatAs.downcase == 'year'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "year",
				# 					"format" : "YYYY"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'month'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "month",
				# 					"format" : "MMM"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'quarter'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "quarter",
				# 					"format" : "YYYY:MM"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'month year'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "month",
				# 					"format" : "YYYY MM"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'week'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "week",
				# 					"format" : "w"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'day'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "day",
				# 					"format" : "E"
				# 				}
				# 			}'
				# elsif formatAs.downcase == 'hour'
				# 	data += '"'+field_name+'" : { 
				# 				"date_histogram" : {'
				# 					if self.get_field_type(type_name, field_name) == "string"
				# 						data += '"field" : "'+field_name+'.raw",'
				# 					else
				# 						data += '"field" : "'+field_name+'",'
				# 					end
				# 					data += '"interval" : "hour",
				# 					"format" : "H"
				# 				}
				# 			}'
				# else
					data += '"'+field_name+'" : { 
								"terms" : {'
									field_type = self.get_field_type(type_name, field_name)
									if field_type == "string"
										data += '"field" : "'+field_name+'.raw",'
									elsif formatAs && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(formatAs.downcase)
										data += '"field" : "'+field_name+'.'+formatAs.downcase+'",'
									else
										data += '"field" : "'+field_name+'",'
									end 
									data += '"size":0
								}
							}'
				# end
				data += '},
							"size":0
						}'
				# formatted_name = self.get_unique_field(field_name,formatAs);		
				curl = Curl::Easy.http_post(url, data) do |c|
					c.headers['Content-type'] = 'text/json'
					c.timeout = 300
				end	
				# debugger
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end
				result = JSON::parse(curl.body_str)
				curl.close unless curl.nil?

				if Cibies::caching_enabled?
					Cibies::cache_set(cache_key, result, Cibies::UNIQUE_KEYS_TIMEOUT) if result
				end
				keys = result["aggregations"][field_name]["buckets"].map do |ent|
					ent["key"]
				end
				Cibies::cleanup
				return keys
			else
				Cibies::cleanup
				return false
			end						
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end	

	def get_data(data_source, type_name, dimensions, factMap, 
		sortMap, sort_order, limit, offset, conditionsMap, 
		display_rank, selectionMap, forecastObject=nil, aggregateObject=nil)
		curl = nil
		# elastic search
		# curl -XPOST 'localhost:9200/1/235_contents/_search?pretty' -d '{ "aggregations" : { "dimension" : { "terms" : { "field" : "Branch Name" }, "aggregations" : {  "depth" : { "terms" : {  "field" : "Loan Type"}, "aggregations" : { "measure 1" : {  "sum" : { "field" : "Loan Amount" } }, "measure 2" : {"avg" : {"field" : "Loan Amount"} } } } } } }, "size":0}'
		begin
			if self.type_exists(type_name)
				fn_start = Time.now.to_f
				last_update_time = data_source.updated_at
				
				# data = '{
				# 			"fields" : [],
				# 			"query" : {},
				# 			"from" : '+offset+',
				# 			"size" : '+limit+',
				# 			"aggs" : {},
				# 			"sort" : {}
				# }'
				data = '{'
				if dimensions || factMap
					data+= self.generate_dimension_aggregations(dimensions.clone, factMap, type_name)
					if limit && limit.to_i > 0
						data+=',"size" : '+limit.to_s+''
					else
						data+=',"size" : 0'
					end					
				end
				if conditionsMap && (conditionsMap.length > 0)
					data += self.get_query(conditionsMap, type_name)
				end
				data+='}'
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/#{type_name}/_search"
				# Get From Cache
				if Cibies::caching_enabled?
					puts("%%%%%%%%%%%%%%%%%%%%%%%%%%%% from caching %%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
					cache_key = Cibies::get_data_key(data, last_update_time)
					result = Cibies::cache_get(cache_key)	
					if result				
						fn_end = Time.now.to_f
						Rails.logger.info "\n ----- Get Data (Cache) Execution Time #{fn_end - fn_start}s ----- \n"
						return result 
					end
				end


				curl = Curl::Easy.http_post(url, data) do |c|
					c.headers['Content-type'] = 'text/json'
					c.timeout = 300
				end	
				# debugger
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return false
				end
				result = JSON::parse(curl.body_str)
				@dim_keys=[]
				dimensions.each do |dim|
					@dim_keys.push(dim["displayName"] || dim["fieldName"])
				end
				@mes_keys=[]
				factMap.each do |mes|
					@mes_keys.push(mes["displayName"] || mes["fieldName"])
				end
				@result_obj={}
				@result_arr=[]
				self.format_result(result["aggregations"])
				curl.close unless curl.nil?
				if Cibies::caching_enabled?
					Cibies::cache_set(cache_key, result, Cibies::GET_DATA_KEY_TIMEOUT) if result
				end

				fn_end = Time.now.to_f
				Rails.logger.info "\n ----- Get Data (No Cache) Execution Time #{fn_end - fn_start}s ----- \n"
				Cibies::cleanup
				return @result_arr
			else
				return false
			end			
		rescue Exception => e
			Cibies::cleanup
			curl.close unless curl.nil?
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return raise e.message
		end
			
	end

	def get_sort_from_measures(factMap)
		factMap.each do |measure|
			if measure["sortOrder"] && !measure["sortOrder"].empty?
				return measure
			end
		end
		return nil
	end

	def get_query(conditionsMap, type_name)
		
		# fields = conditionsMap.map do |condition|
		# 	condition["fieldName"]
		# end

		query_str = self.get_query_array(conditionsMap, type_name)

		aggs = ', "query" : {'
		aggs += '"bool": {'
		aggs += '"must": '
		aggs += query_str
		# aggs += '{'
		# aggs += '"query_string" : {'
		# aggs += '"fields" : '+fields.to_s+','
		# aggs += '"query" : "'+query_str+'"'
		# aggs += '}'
		# aggs += '}'
		# aggs += ']'
		aggs += '}'
		aggs += '}'
		return aggs
	end

	def get_query_array(conditionsMap, type_name)
		# debugger
		query_str = '['
		conditionsMap.each do |condition|
			if query_str.rindex('[') != (query_str.length - 1)
				query_str += ','
			end
			if condition["comparision"] != "RANGE"
				if condition["value"].class == Array
					query = "("
					condition["value"].each do |val|
						if query.rindex('(') != (query.length - 1)
							query += " OR "
						end
						query += '\"'+val+'\"'
					end
					query += ")"
				else
					query = condition["value"].to_s
				end
			end
			case condition["comparision"]
			when "IN"
				query_str += '{'
				query_str += '"query_string" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end				
				query_str += '"query" : "'+query+'"' 
				query_str += '}'
				query_str += '}'
			when "NOT IN"
				query_str += '{'
				query_str += '"query_string" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end
				query_str += '"query" : "NOT'+query+'"' 
				query_str += '}'
				query_str += '}'
			when "<"
				query_str += '{'
				query_str += '"query_string" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end
				query_str += '"query" : "<'+query+'"' 
				query_str += '}'
				query_str += '}'
			when ">"
				query_str += '{'
				query_str += '"query_string" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end
				query_str += '"query" : ">'+query+'"' 
				query_str += '}'
				query_str += '}'
			when "IS NOT NULL"
				query_str += '{'
				query_str += '"filtered" : {'
				query_str += '"filter" : {'
				query_str += '"exists" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end
				query_str += '}' 
				query_str += '}'
				query_str += '}'
				query_str += '}'
			when "RANGE"
				query_str += '{'
				query_str += '"query_string" : {'
				if self.get_field_type(type_name, condition["fieldName"]) == "string"
					query_str += '"default_field" : "'+condition["fieldName"]+'.raw",'
				elsif condition["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(condition["formatAs"].downcase)
					query_str += '"default_field" : "'+condition["fieldName"]+'.'+condition["formatAs"].downcase+'",'
				else
					query_str += '"default_field" : "'+condition["fieldName"]+'",'
				end
				query_str += '"query" : "[\"'+condition["value"][0].strftime("%Y/%m/%d %H:%M:%S").to_s+'\" TO \"'+condition["value"][1].strftime("%Y/%m/%d %H:%M:%S")+'\"]"' 
				query_str += '}'
				query_str += '}'
			end

			# if !query_str.empty?
			# 	query_str += " AND "
			# end
			# query_str += "("
			# condition["value"].each do |val|
			# 	if query_str.rindex('(') != (query_str.length - 1)
			# 		query_str += " OR "
			# 	end
			# 	query_str += val
			# end
			# query_str += ")"
		end
		query_str += ']'
		return query_str
	end

# 	"aggregations" : {
# 		"Branch Name" : {
# 			"terms" : {
# 				"field" : "Branch Name"
# 			},
# 			"aggregations" : {
# 				"Loan Type" : {
# 					"terms" : {
# 						"field" : "Loan Type"
# 					},
# 					"aggregations" : {
# 						"Sum of Loan Amount" : {
# 							"sum" : {
# 								"field" : "Loan Amount"
# 							}
# 						}
# 					}
# 				},
# 				"aggregations" : {
# 					"Sum of Loan Amount" : {
# 						"sum" : {
# 							"field" : "Loan Amount"
# 						}
# 					}
# 				}
# 			}
# 		},
# 		"aggregations" : {
# 			"Sum of Loan Amount" : {
# 				"sum" : {
# 					"field" : "Loan Amount"
# 				}
# 			}
# 		}
# 	},
# 	"size" : 0
# }

	def format_result(res, count=0)
		# formatting results from elastic search call to match the response of mysql
		if @dim_keys && @dim_keys.length > 0
			d = @dim_keys[count]
			res[d]['buckets'].each do |bucket|
				if bucket['key_as_string']
					@result_obj[d] = bucket['key_as_string']
				else
					@result_obj[d] = bucket['key']
				end
				if @dim_keys[count + 1]
					self.format_result(bucket, (count+1)) 
				else
					@mes_keys.each do |mes|
						@result_obj[mes] = bucket[mes]["value"]
					end			
					@result_arr.push(@result_obj.clone)
				end
			end
		elsif @mes_keys && @mes_keys.length > 0
			@mes_keys.each do |mes|
				@result_obj[mes] = res[mes]["value"]
			end
			@result_arr.push(@result_obj.clone)
		end
	end

	# 	res = {
	# 			"Branch Name" => {
	# 				"buckets"=>[
	# 					{
	# 						"key" => "bhandara", 
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>13187.0
	# 									}
	# 								}, 
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>14574.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					}, 
	# 					{
	# 						"key"=>"chandrapur", 
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>13348.0
	# 									}
	# 								}, 
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>13511.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					}, 
	# 					{
	# 						"key"=>"gadchiroli", 
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>13637.0
	# 									}
	# 								}, 
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>12976.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					}, 
	# 					{
	# 						"key"=>"nagpur", 
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>5756.0
	# 									}
	# 								},
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>7737.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					},
	# 					{
	# 						"key"=>"wardha", 
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>13138.0
	# 									}
	# 								},
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>14740.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					},
	# 					{
	# 						"key"=>"yavatmal",
	# 						"doc_count"=>540, 
	# 						"Loan Type"=>{
	# 							"buckets"=>[
	# 								{
	# 									"key"=>"car", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>14274.0
	# 									}
	# 								},
	# 								{
	# 									"key"=>"home", 
	# 									"doc_count"=>270, 
	# 									"Sum of Loan Amount"=>{
	# 										"value"=>14006.0
	# 									}
	# 								}
	# 							]
	# 						}
	# 					}
	# 				]
	# 			}
	# 		}
	# end

	def generate_dimension_aggregations(dimensions, factMap, type_name)
		aggs = '"aggregations" : {'
		if dimensions && dimensions.length > 0
			dimension=dimensions.shift
			aggregation_type = self.get_aggregation_type(dimension["formatAs"])
			aggs += '"'+dimension["displayName"]+'" : {'
			aggs += '"'+ aggregation_type + '" : {'
			field_type = self.get_field_type(type_name, dimension["fieldName"])
			if field_type == "string"
				aggs += '"field" : "'+dimension["fieldName"]+'.raw"'
			elsif dimension["formatAs"] && ["month", "quarter", "year", "month year", "day", "week", "hours"].include?(dimension["formatAs"].downcase)
				aggs += '"field" : "'+dimension["fieldName"]+'.'+dimension["formatAs"].downcase+'"'
			else
				aggs += '"field" : "'+dimension["fieldName"]+'"'
			end
			sort_measure = self.get_sort_from_measures(factMap)
			if sort_measure
				aggs += ','
				aggs += '"order" : {"'+sort_measure["displayName"]+'" : "'+sort_measure["sortOrder"]+'"}'
			elsif dimension["sortOrder"] && !dimension["sortOrder"].empty?
				aggs += ','
				aggs += '"order" : {"_term" : "'+dimension["sortOrder"]+'"}'
			end
			if aggregation_type == "date_histogram"
				aggs += ',"interval" : "' + ((dimension["formatAs"].downcase == "month year") ? "month" : dimension["formatAs"].downcase) + '"'
				aggs += ',"format" : "'+ self.get_format(dimension["formatAs"].downcase) +'"'
			elsif aggregation_type == "terms"
				aggs += ',"size" : 0'
			end
			aggs += '}'
			if dimensions.present?
				aggs +=','
				aggs += self.generate_dimension_aggregations(dimensions, factMap, type_name)
			else
				aggs +=','
				aggs += self.generate_measure_aggregations(factMap, type_name)
			end
			aggs += '}'
			aggs +=','
			aggs += self.generate_measure_aggregations(factMap, type_name, false)
		elsif factMap			
			aggs += self.generate_measure_aggregations(factMap, type_name, false)	
		end
		aggs += '}'


		# "aggregations":{
		# 	"dimension" : { 
		# 		"terms" : { 
		# 			"field" : "Branch Name" 
		# 		}, 
		# 		"aggregations" : {  
		# 			"depth" : { 
		# 				"terms" : {  
		# 					"field" : "Loan Type"
		# 				}, 
		# 				"aggregations" : { 
		# 					"measure 1" : {  
		# 						"sum" : { 
		# 							"field" : "Loan Amount" 
		# 						} 
		# 					} ,
		# 					"measure 2" : {  
		# 						"avg" : { 
		# 							"field" : "Loan Amount" 
		# 						} 
		# 					} 
		# 				} 
		# 			},
		# 			"measure 1" : {
		# 				"sum" : { 
		# 					"field" : "Loan Amount" 
		# 				} 
		# 			},
		# 			"measure 2" : {  
		# 				"avg" : { 
		# 					"field" : "Loan Amount" 
		# 				}
		# 			}
		# 		} 
		# 	},
		# 	"measure 1" : {
			# 	"sum" : { 
			# 		"field" : "Loan Amount" 
			# 	} 
			# },
			# "measure 2" : {  
			# 	"avg" : { 
			# 		"field" : "Loan Amount" 
			# 	}
			# }
		# }, 
		# "size":0, 
		# "query" : {
		# 	"query_string" : { 
		# 		"default_field" : "Branch Name", 
		# 		"query" : "nagpur bhandara" 
		# 	}
		# }

		# curl -XPOST 'localhost:9200/1/235_contents/_search?pretty' -d '{ "aggregations" : { "dimension" : { "terms" : { "field" : "Branch Name", "order" : {"measure 1" : "desc"} }, "aggregations" : {  "depth" : { "terms" : {  "field" : "Loan Type"}, "aggregations" : { "measure 1" : {  "sum" : { "field" : "Loan Amount" } }, "measure 2" : {"avg" : {"field" : "Loan Amount"} }} }, "measure 1" : {"sum" : {"field" : "Loan Amount"}}, "measure 2" : {"avg" : {"field" : "Loan Amount"}} } } }, "size":0, "query" : {"query_string" : { "default_field" : "Branch Name", "query" : "nagpur bhandara" }}}'
	end

	def get_format(formatAs)
		case formatAs			
		when "month"
			return "MM"
		when "year"
			return "YYYY"
		when "month year"
			return "YYYY MM"
		when "quarter"
			return "YYYY:MM"
		when "week"
			return "w"
		when "day"
			return "E"
		when "hour"
			return "H"				
		end
	end

	def generate_measure_aggregations(factMap, type_name, isNested=true)
		aggs=''
		if factMap.present?
			if isNested
				aggs+='"aggregations" : {'
			end
			factMap.each do |measure|
				aggregation_type = self.get_aggregation_type(measure["formatAs"])
				aggs += '"'+(measure["displayName"] || measure["fieldName"]) + '" : {'
				aggs += '"'+ aggregation_type + '" : {'
				if self.get_field_type(type_name, measure["fieldName"]) == "string"
					aggs += '"field" : "'+measure["fieldName"]+'.raw"'
				else
					aggs += '"field" : "'+measure["fieldName"]+'"'
				end
				if aggregation_type == "terms"
					aggs += ', "size" : 0'
				end
				aggs+='}'
				aggs+='},'
			end
			aggs=aggs[0...-1]
			if isNested
				aggs+='}'
			end
		end
		aggs

		# "aggregations" : { 
		# 					"measure 1" : {  
		# 						"sum" : { 
		# 							"field" : "Loan Amount" 
		# 						} 
		# 					} ,
		# 					"measure 2" : {  
		# 						"avg" : { 
		# 							"field" : "Loan Amount" 
		# 						} 
		# 					} 
		# 				} 
	end

	def get_aggregation_type(formatAs)
		if formatAs.nil? || formatAs.strip.empty?
			return "terms"
		elsif formatAs.downcase == "count"
			return "value_count"				
		elsif  ["month", "year", "month year", "quarter", "week", "day", "hour"].include?(formatAs.downcase)
			return "terms"
		elsif ["sum","avg","min","max"].include?(formatAs.downcase)
			return formatAs.downcase
		elsif ["stddev_pop", "var_pop"].include?(formatAs.downcase)
			return "extended_stats"	
		end
	end

	def get_field_type(type_name, field_name)
		curl = nil
		begin
			if self.type_exists(type_name)
				url =  "#{Cibies::URL}/#{Cibies::INDEX}/_mapping/#{type_name}/field/#{field_name}"
				curl = Curl::Easy.http_get(url) do |c|
					c.timeout = 300
				end
				if curl.response_code != 200  || JSON.parse(curl.body_str)["errors"] == true
					curl.close unless curl.nil?
					Cibies::cleanup
					puts "Curl Returned an error response code. #{curl.body_str}"
					Rails.logger.error "Curl Returned an error response code #{curl.body_str}"
					return nil
				end	
				result=JSON.parse(curl.body_str)		
				curl.close unless curl.nil?
			end
			Cibies::cleanup
			return result[Cibies::INDEX]["mappings"][type_name][field_name]["mapping"][field_name]["type"]
		rescue Exception => e
			curl.close unless curl.nil?
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
	end

	def self.get_data_key(data, time) 
		hash = Digest::MD5.hexdigest(data)
		return "get_data_key_#{hash}_#{time.to_s}"
	end

	protected
	def self.cache_get(key)
		if $redis
			val = $redis.get(key)
			return val ? JSON.parse(val) : val
		else
			puts "\n[ERROR] [CRITICAL] Cache not initialized!"			
		end 
	end	

	def self.cache_set(key, value, timeout)
		if $redis
			$redis.setex(key, timeout, value.to_json)
		else
			puts "\n[ERROR] [CRITICAL] Cache not initialized!"			
		end 
	end

	def self.caching_enabled?
		return Rails.env == "production" || $default_caching_state
	end

end
