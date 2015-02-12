class Account < ActiveRecord::Base
  attr_accessible :api_token, :id, :name, :store_url

  	def fetch_data
	    cibies= Cibies.new
	    cibies.instance_variable_set(:@index, "index_#{self.id}")
	    cibies.fetch_data(self)
  	end

  	def get_data(report_name)
  		case report_name.upcase
  		when "TOTAL ORDERS"
  			query = '{"query": {"match_all": {}}, "size":0 }'
  		when "TOTAL SALES"
  			query = '{"size":0, "aggs" : {"total_sales" : {"sum" : {"field" : "total"}}}}'
  		when "TOTAL CUSTOMERS"
  			query = '{"size":0, "aggs" : {"total_customers" : {"cardinality" : {"field" : "order.bill_address.full_name" }}}}'
  		when "LATEST ORDERS"
  			query = '{"size":7, "sort" : {"created_at" : "desc" }}'
  		when "ABONDONED CARTS"
  			query = '{"query":{"match":{"state":"cart"}}}'
  		when "SALES TIMELINE"
  			query = '{"size" : 0,"aggregations" :{"x-dim" : {"terms" : {"field" : "created_at","size" : 0}, "aggregations" : {"Count of quantity" : {"sum" : {"field" : "total"}}}}}}'
  		when "SALES REPORT"
  			query = '{"size":0, "aggregations":{"date_agg":{"date_histogram":{"field":"created_at", "interval":"month", "format":"MMM"}, "aggregations": { "no_of_products":{ "sum": { "field" : "total_quantity"} }, "tax" : { "sum" : { "field" : "tax_total"} }, "total" : { "sum" : { "field" : "total" } } } } } }'
  		when "SHIPPING REPORT"
  			query = '{"size":0, "aggregations":{"date_agg":{"date_histogram":{"field":"created_at", "interval":"month", "format":"MMM"}, "aggregations": { "shipping_title":{ "terms": { "field" : "shipment_state"}, "aggregations" : {"Total" : {"sum" : {"field" : "total"} } } } } } } }'
  		when "RETURNS REPORT"
  			query = '{"size":0, "aggregations":{"date_agg":{"date_histogram":{"field":"created_at", "interval":"day", "format":"yyyy-MMM-dd"} } }, "query" : {"match":{"state":"return"}} }'
  		when "PRODUCTS PURCHASED REPORT"
  			query = '{"size":0, "aggregations" : { "product name" : { "terms" : { "field" : "order.line_items.variant.name" , "order" : { "quantity" : "desc"} }, "aggregations" : {"quantity" : { "sum" : { "field" : "order.line_items.quantity" } }, "total" : { "sum" : { "field" : "order.line_items.total" } } } } } }'
  		when "CUSTOMER ACTIVITY"
  			query = '{"aggregations" : { "first name" : { "terms" : { "field" : "order.bill_address.firstname" }, "aggregations" : { "last name" : { "terms" : { "field" : "order.bill_address.lastname" }, "aggregations" : { "Total quantity" : { "sum" : { "field" : "order.total_quantity" } }, "Total" : { "sum" : { "field" : "order.total" } } } } } } }, "size" : 0 }'
  		cibies= Cibies.new
	    cibies.instance_variable_set(:@index, "index_#{self.id}")
	    cibies.query_data(query)
  	end

end
