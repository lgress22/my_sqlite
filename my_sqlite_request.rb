require 'csv'

class MySqliteRequest
  def initialize
    @table_name = []
    @columns = []
    @conditions = []
    @joins = []
    @order_by = nil
    @order_direction = 'ASC'
    @insert_table = nil
    @insert_data = []
    @insert_mode = false
    @update_table = nil
    @update_mode = false
    @update_data = {}
    @file_name = nil
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

  def update(table_name)
    @update_table = table_name
    @update_mode = true
    self
  end

  def set(column, value)
    @update_data[column] = value
    self
  end

  def read_csv_file(table_name)
    @csv_files = ['test.csv', 'database.csv']
  
    csv_data_combined = [] # Combined data from all CSV files
  
    @csv_files.each do |csv_file|
      csv_data = []
  
      CSV.foreach(csv_file, headers: true) do |row|
        csv_data << row
      end
  
      csv_data_combined += csv_data # Combine data from each CSV file
    end
  
    puts "Is this working?"
  
    csv_data_combined.each do |row|
      puts row.inspect
    end
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

      data = data.select { |row| row[column.to_i] == value }
    end

    data
  end

  def apply_column(data)
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

  def apply_insert
    return if @csv_files.nil? || @csv_files.empty?
  
    @csv_files.each do |csv_file|
      existing_data = read_csv_file(csv_file)
      combined_data = existing_data + @insert_data
  
      CSV.open(csv_file, 'wb') do |csv|
        csv << combined_data.first.headers
        combined_data.uniq! # Remove duplicates
        combined_data.each do |row|
          csv << row
        end
      end
    end
  
    @insert_data = [] # Clear insert data after writing
  end
  

  def apply_update(data)
    return unless @update_mode
  
    data_by_col = data.by_col # Convert data into columns
  
    data.each_with_index do |row, index|
      if row[@conditions[0][:column]] == @conditions[0][:value]
        @update_data.each do |column, value|
          data_by_col[column][index] = value
        end
      end
    end
  
    CSV.open(@table_name, 'wb', headers: data.headers) do |csv|
      csv << data.headers
      data.each { |row| csv << row }
    end  
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
    data = apply_column(data)
    data = apply_order(data)
    apply_insert
    data = apply_update(data)
    
    data
  end
  

  private
end
  

  

request = MySqliteRequest.new

# request.from('test.csv')
#        .select('Player', 'firstname')
#        #.join('ID', 'database.csv', 'ID')
#        #.where('Player', 'Curly Armstrong')
#        #.order('Player', 'DESC')
#   results = request.run

  

 request.insert('database.csv')
        .values(['4', 'Jill', 'Pilinsky', '35', 'El Dorado'])
        results = request.run

# request.update('database.csv')
#     request.set('firstname', 'Jim')
#     .set('lastname', 'Wilson')
#     .where('ID', '2')
#      results = request.run('database.csv')




     results.each do |row|
      puts row.inspect
    end