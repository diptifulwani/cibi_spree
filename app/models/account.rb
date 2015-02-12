class Account < ActiveRecord::Base
  attr_accessible :api_token, :id, :name, :store_url

  	def fetch_data
	    cibies= Cibies.new
	    cibies.instance_variable_set(:@index, "index_#{self.id}")
	    cibies.fetch_data(self)
  	end

end
