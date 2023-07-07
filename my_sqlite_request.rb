require 'csv'

class MySqliteRequest
    def initialize
        @table_name = nil
        @select_columns = []
        @where_conditions = []
        @join_table = nil
        @join_condition = nil
        @order = nil
        @insert_data = nil
        @conditions = {}
    end
    

    def self.from(table_name)
        new.from(table_name)
    end
    

    def from(table_name)
        @table_name = table_name
        self
    end
    

    def insert(table_name)
        @table_name = table_name
        self
    end
  
    def self.insert(table_name)
        new.insert(table_name)
    end
  

    def select(*column_names)
        @select_columns = column_names.flatten
        self
    end


  def where(column_name, criteria)
    @where_conditions << { column_name: column_name, criteria: criteria }
    self
  end
  
  

  def join(column_a, table_b, column_b)
    @join_table = table_b
    @join_condition = { column_a: column_a, table_b: table_b, column_b: column_b }
    self
  end

  def order(order, column_name)
    @order = "#{column_name} #{order}"
    self
  end

  def values(data)
    @insert_data = data
    self
  end

  def run
    result = []
  
    database_data = CSV.read('database.csv', headers: true)
    test_data = CSV.read('test.csv', headers: true)
  
    database_columns = @select_columns.select { |column| column.start_with?('database.') }.map { |column| column.split('.')[1] }
    test_columns = @select_columns.select { |column| column.start_with?('test.') }.map { |column| column.split('.')[1] }
  
    test_data.each do |test_row|
      join_rows = database_data.select { |database_row| database_row[@join_condition[:column_a]] == test_row[@join_condition[:column_b]] }
      join_rows.each do |database_row|
        result_row = {}
        database_columns.each do |column|
          result_row[column] = database_row[column]
        end
        test_columns.each do |column|
          result_row[column] = test_row[column]
        end
        result << result_row
      end
    end
  
    result.each do |row|
      puts row
    end
  
    result
  end
  
    private

  def evaluate_conditions(row)
    @where_conditions.all? do |condition|
      column_name = condition[:column_name]
      criteria = condition[:criteria]
      row[column_name] == criteria
    end
  end

  

  def append_data_to_csv
    CSV.open(@table_name, 'a') do |csv|
      csv << @insert_data.values
    end

    #puts 'Data inserted successfully.'
  end
end


request = MySqliteRequest.new
request.select('database.lastname', 'test.Player')
request.from('database.csv')
request.join('ID', 'test.csv', 'ID')

request.run()
