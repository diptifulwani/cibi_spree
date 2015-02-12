class CreateAccounts < ActiveRecord::Migration
  def change
    create_table :accounts do |t|
      t.integer :id
      t.string :name
      t.string :store_url
      t.string :api_token

      t.timestamps
    end
  end
end
