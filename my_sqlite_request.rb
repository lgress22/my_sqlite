require 'sqlite3'
require 'csv'

class MySqliteRequest
  def initialize
    @table_name = nil
    @select_columns = []
    @where_conditions = []
    @join_table = nil
    @join_condition = nil
    @order = nil
  end

  def from(table_name)
    @table_name = table_name
    self
  end

  def select(*column_names)
    @select_columns = column_names.flatten
    self
  end

  def where(*conditions)
    @where_conditions = conditions.flatten
    self
  end

  def join(column_a, table_b, column_b)
    @join_table = table_b
    @join_condition = "#{@table_name}.#{column_a} = #{@join_table}.#{column_b}"
    self
  end

  def order(order, column_name)
    @order = "#{column_name} #{order}"
    self
  end

  def run
    select_columns = @select_columns.join(', ')
    query = "SELECT #{select_columns} FROM #{@table_name} #{generate_conditions};"
    db = SQLite3::Database.new(@database_name)
    db.execute(query) do |row|
      puts row.join(', ')
    end
  end  
end

MySqliteRequest.new.select('Player').from('test.csv').where("Player = 'Cliff Barker'").run




