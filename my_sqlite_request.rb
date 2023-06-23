require 'csv'

class MySqliteRequest
  def initialize
    # db = SQLite3::Database.new("database.db")
    @table_name = nil
    @select_columns = []
    @where_conditions = []
    @join_table = nil
    @join_condition = nil
    @order = nil
    @insert_data = nil
    
  end

#   def create_table
#     db.execute("CREATE TABLE IF NOT EXISTS test (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         Player TEXT,
#         Height INT,
#         Weight INT,
#         Collage TEXT,
#         Born INT,
#         Birth_city TEXT,
#         Birth_state TEXT)")
#   end

  def from(table_name)
    @table_name = table_name
    self
  end

  def self.from(table_name)
    new.from(table_name)
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

  def insert(table_name)
    @table_name = table_name
    self
  end

  def self.insert(table_name)
    new.insert(table_name)
  end

  def values(data)
    @insert_data = data
    self
  end

#   def generate_conditions
#     conditions = []
#     conditions << "WHERE " + @where_conditions.join(' AND ') unless @where_conditions.empty?
#     conditions << "JOIN #{@join_table} ON #{@join_condition}" if @join_table && @join_condition
#     conditions.join(' ')
#   end  

   def run
    result = []

    CSV.foreach(@table_name, headers: true) do |row|
        if @where_conditions.empty? || evaluate_conditions(row)
          result_row = {}
          @select_columns.each { |column| result_row[column] = row[column] }
          result << result_row
        end
    end
    result.each do |row|
        puts row
      end
  
    result
    
   end  

   def evaluate_conditions(row)
        @where_conditions.all? do |condition|
            eval(condition, binding)
        end
    end
end

MySqliteRequest.new.select('Birth_city').from('test.csv').where("row['Player'] == 'Cliff Barker'").run


MySqliteRequest.insert('database.csv').values('firstname' => "Thomas", 'lastname' => "Anderson", 'age' => 33, 'password' => 'matrix').run()

