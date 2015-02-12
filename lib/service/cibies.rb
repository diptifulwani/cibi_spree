class Cibies
	URL = "localhost:9200"
	BATCH_SIZE = 15
	GET_DATA_KEY_TIMEOUT = 43200  # 30 Minutes

	def self.cleanup
		curl = Curl::Easy.new
		curl.close		
		GC.start
	end

	def fetch_data(account)
		time = Time.now - 5.minutes #need to be discussed and changed accordingly
		url= account.store_url + '/api/orders.json?token=' + account.api_token
      	orders = Curl.get(url)
      	order_tokens = get_order_tokens(orders.body_str)
      	order_tokens.each do |order_number,order_token|
          fetch_and_save_spree_sales(order_number, order_token, account)
        end
	end

	def get_order_tokens(response_str)
	    data_hash = JSON.parse(response_str)
	    order_tokens = {}
	    data_hash["orders"].collect{|h| order_tokens[h['number']] = h['token']}
	    order_tokens
  	end

  	def fetch_and_save_spree_sales(order_number, order_token, account)
      	begin
			orders_url= account.store_url + '/api/orders/'+order_number+'?order_token=' + order_token + "&token=#{account.api_token}"
			orders = Curl.get(orders_url)
			add_order(orders.body_str, order_number)
      	rescue Exception => e
			Rails.logger.error e.to_s
			puts e.to_s
      	end
  	end

  	def add_order(order, order_number)
  		type_name = "order"
  		url =  "#{Cibies::URL}/#{@index}/#{type_name}/#{order_number}/"
		failed_curls = [], succeeded_curls = []	
		begin
			str=""
			str+=order
			curl = Curl::Easy.http_post(url, str) do |c|
				c.headers['Content-type'] = 'text/json'
				c.timeout = 300
			end
			if (curl.response_code != 200 && curl.response_code != 201) || JSON.parse(curl.body_str)["errors"] == true
				msg=curl.body_str
				curl.close unless curl.nil?
				puts "Curl Returned an error response code. #{curl.body_str}"
				Rails.logger.error "Curl Returned an error response code"
				return msg
			end
			result = curl.body_str

			curl.close unless curl.nil?								
		rescue Exception => e
			Cibies::cleanup
			puts "Curl Failed #{e.message}"
			puts "#{e.backtrace}"
			Rails.logger.error "Curl Failed #{e.message}"
			return false
		end
  	end

  	def query_data(data)
  		url = "#{Cibies::URL}/#{@index}/#{type_name}/_search"
  		curl = Curl::Easy.http_post(url, data) do |c|
			c.headers['Content-type'] = 'text/json'
			c.timeout = 300
		end

		if (curl.response_code != 200 && curl.response_code != 201) || JSON.parse(curl.body_str)["errors"] == true
			msg=curl.body_str
			curl.close unless curl.nil?
			puts "Curl Returned an error response code. #{curl.body_str}"
			Rails.logger.error "Curl Returned an error response code"
			return msg
		end
		result = JSON::parse(curl.body_str)
  	end
end