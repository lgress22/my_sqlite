require 'csv'

class MySqliteRequest
  def initialize
    @table_name = nil
    @select_columns = []
    @where_conditions = []
    @join_table = nil
    @join_condition = nil
    @order = nil
    @insert_data = []
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
    database_data = CSV.read(@table_name, headers: true)
    @selected_data = database_data.map do |row|
      selected_row = {}
      column_names.each do |column_name|
        selected_row[column_name] = row[column_name]
      end
      selected_row
    end
    self
  end
  

  def where(column_name, criteria)
    @where_conditions << { column_name: column_name, criteria: criteria }
    self
  end

  def join(column_a, table_b, column_b)
    @join_table = table_b
    @join_condition = { column_a: column_a, column_b: column_b }
    self
  end

  def order(order, column_name)
    @order = "#{column_name} #{order}"
    self
  end

  def values(data)
    @insert_data << data
    self
  end

  def run
    result = []
  
    database_data = CSV.read('database.csv', headers: true)
    test_data = CSV.read('test.csv', headers: true)
  
    database_columns = @select_columns.select { |column| column.start_with?('database.') }.map { |column| column.split('.')[1] }
    test_columns = @select_columns.select { |column| column.start_with?('test.') }.map { |column| column.split('.')[1] }
  
    if @join_condition
      join_rows = database_data.select do |database_row|
        test_data.any? do |test_row|
          database_row[@join_condition[:column_a]] == test_row[@join_condition[:column_b]]
        end
      end
  
      join_rows.each do |database_row|
        test_rows = test_data.select { |test_row| database_row[@join_condition[:column_a]] == test_row[@join_condition[:column_b]] }
        test_rows.each do |test_row|
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
    else
      result_row = {}
      database_columns.each do |column|
        result_row[column] = nil
      end
      result << result_row
    end
  
    if result.empty?
        puts "No results found."
      else
        result.each do |row|
          puts row unless row.empty?
        end
      end
      
  
    append_data_to_csv if @insert_data.any?
  
    result
  end
  

  private

  def append_data_to_csv
    CSV.open(@table_name, 'a') do |csv|
      csv << [] if File.zero?(@table_name) # Append an empty row if the file is empty
      @insert_data.each do |data|
        csv << data
      end
    end
  
    puts 'Data inserted successfully.'
  end
  
end

request = MySqliteRequest.new

request.select('firstname').from('database.csv')
# request.insert('database.csv')
# request.values(['2', 'John', 'Doe', '30', 'New York'])
# request.values(['3', 'Jane', 'Smith', '25', 'Los Angeles'])
 request.run
