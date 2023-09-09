require 'csv'
class MySqliteRequest
  def initialize
    @table_name = nil
    @columns = []
    @conditions = []
    @joins = []
    @order_by = nil
    @order_direction = 'ASC'
    @insert_table = nil
    @insert_data = []
    @insert_mode = false
  end

  def from(table_name)
    @table_name = table_name
    self
  end
  def select(*columns)
    @columns = columns.flatten
    self
  end
  def where(column, value)
    @conditions << { column: column, value: value }
    self
  end
  def join(column_a, table_b, column_b)
    @joins << { column_a: column_a, table_b: table_b, column_b: column_b }
    self
  end
  def order(column, direction = 'ASC')
    @order_by = column
    @order_direction = direction.upcase
    self
  end

  def insert(table_name)
    @insert_table = table_name
    @insert_mode = true
    self
  end

  def values(data)
    @insert_data << data
    self
  end


  def read_csv_file(file_name)
    CSV.read(file_name, headers: true)
  end
  def apply_join(data, join_data, column_a, column_b)
    join_data_hash = Hash[join_data.map { |row| [row[column_b.to_s], row] }]
    data.map do |row|
      join_value = row[column_a.to_s]
      joined_row = row.to_h.merge(join_data_hash[join_value].to_h)
      joined_row.delete(column_b.to_s) # Remove duplicate column
      joined_row
    end
  end
  def apply_conditions(data)
    return data if @conditions.empty?
    @conditions.each do |condition|
      column = condition[:column]
      value = condition[:value]
      data = data.select { |row| row[column.to_s] == value }
    end
    data
  end
  def apply_column_selection(data)
    if @columns.empty?
      data
    else
      data.map do |row|
        row.select { |column, _| @columns.include?(column) }
      end
    end
  end
  def apply_order(data)
    return data unless @order_by
    sorted_data = data.sort_by { |row| row[@order_by.to_s] }
    sorted_data.reverse! if @order_direction == 'DESC'

    sorted_data
  end

  def execute_insert
    # Read the original data
    data = read_csv_file(@insert_table)

    # Append the new rows to the original data
    @insert_data.each do |row|
      data << CSV::Row.new(data.headers, row)
    end

    # Write the updated data back to the CSV file
    CSV.open(@insert_table, 'wb') do |csv|
      csv << data.headers
      data.each do |row|
        csv << row
      end
    end

    @insert_data = [] # Reset insert_data after executing the insert
  end

  def run
    data = read_csv_file(@table_name)
    @joins.each do |join|
      column_a = join[:column_a]
      table_b = join[:table_b]
      column_b = join[:column_b]
      join_data = read_csv_file(table_b)
      data = apply_join(data, join_data, column_a, column_b)
    end
    data = apply_conditions(data)
    data = apply_column_selection(data)
    data = apply_order(data)

    if @insert_mode
      execute_insert
      @insert_mode = false # Reset insert_mode after executing the insert
    end

    data
  end
  private
end

request = MySqliteRequest.new

# request.from('test.csv')
#        .select('Player', 'firstname')
#        .join('ID', 'database.csv', 'ID')
#        #.where('Player', 'Curly Armstrong')
#        #.order('Player', 'DESC')
# results = request.run

request.insert('database.csv')
        .values(['3', 'John', 'Smith', '33', 'Wisconsin'])

results.each do |row|
  puts row.inspect
end