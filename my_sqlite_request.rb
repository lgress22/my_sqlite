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
    @update_table = nil
    @update_mode = false
    @update_data = {}
    @delete_mode = false
    @delete_table = nil
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

  def delete(table_name)
    @delete_table = table_name
    @delete_mode = true
    self
  end

  def read_csv_file(file_name)
    CSV.read(file_name, headers: true)
  end

  # def read_csv_file(table_name)
  #   @csv_files = ['database.csv']
  
  #   csv_data_combined = [] # Combined data from all CSV files
  
  #   @csv_files.each do |csv_file|
  #     csv_data = []
  
  #     CSV.foreach(csv_file, headers: true) do |row|
  #       csv_data << row
  #     end
  
  #     csv_data_combined += csv_data # Combine data from each CSV file
  #   end
  
  #   #puts "Is this working?"
  
  #   csv_data_combined.each do |row|
  #   end
  # end
   
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
  
    sorted_data = data.sort_by { |row| row[@order_by] }
  
    sorted_data = sorted_data.reverse if @order_direction == 'DESC'
  
    sorted_data
  end
  

  def apply_insert(table_name)
    return if table_name.nil?
    
    csv_file = "database.csv"
    existing_data = read_csv_file(csv_file)
    
    # Convert existing data to arrays for comparison
    existing_data_arrays = existing_data.map(&:fields)
    
    new_data = @insert_data.reject do |new_row|
      existing_data_arrays.any? { |existing_row| existing_row == new_row }
    end
    
    combined_data = new_data
    
    # Write combined data to CSV
    CSV.open(csv_file, 'a') do |csv| # Write headers from existing data
      combined_data.each do |row|
        csv << row

        # p existing_data
        # p new_data
        # p combined_data
      end
    end
      return existing_data
    end

    def apply_update(data)
      csv_file = "database.csv"
      existing_data = read_csv_file(csv_file)
    
      data.each do |row|
        id = row['ID']
        existing_row = existing_data.find { |existing_row| existing_row['ID'] == id.to_s }

        if existing_row
          # Check if the existing row meets the WHERE condition
          if @conditions.all? { |condition| existing_row[condition[:column]] == condition[:value] }
            @update_data.each do |column, value|
              existing_row[column] = value if existing_row[column]
            end
          end
        end
      end

      CSV.open(csv_file, 'w') do |csv|
        csv << existing_data.first.headers   
        existing_data.each do |row|
          csv << row.fields
        end
      end
      
      existing_data
    end
    
    def apply_delete(data)
      return data unless @delete_mode # Check if delete mode is enabled
      
      csv_file = "database.csv"
      existing_data = read_csv_file(csv_file)
    
      deleted_ids = [] # Keep track of deleted IDs
    
      existing_data.delete_if do |row|
        id = row['ID']
        if @conditions.all? { |condition| row[condition[:column]] == condition[:value] }
          deleted_ids << id.to_i
          puts "Deleted row with ID: #{id}"
          true # Remove the row
        else
          false # Keep the row
        end
      end
    
      # Renumber the ID column for the remaining rows
      remaining_id = 0 # Start ID numbering from 0
      existing_data.each do |row|
        row['ID'] = remaining_id.to_s
        remaining_id += 1
      end
    
      # Write updated data to CSV
      CSV.open(csv_file, 'w') do |csv|
        csv << existing_data.first.headers
        existing_data.each do |row|
          csv << row.fields
        end
      end
    
      @delete_mode = false # Disable delete mode after the operation
    
      return existing_data
    end
    
    
    
    
    
         
  def run
    csv_file = "database.csv"
    data = read_csv_file(csv_file)
    # puts "Initial data:"
    # puts data.inspect

    @joins.each do |join|
      column_a = join[:column_a]
      table_b = join[:table_b]
      column_b = join[:column_b]
  
      join_data = read_csv_file(table_b)
      data = apply_join(data, join_data, column_a, column_b)
    end

    #data = apply_column_selection(data)
  
     
     #p "After applying conditions"
     #p data.inspect

     data = apply_column(data)
      # p "after applying columns"
      # p data.inspect

       #data = apply_conditions(data)
      # p data.inspect

    #  data = apply_order(data)
    # # # p "After applying order"
    # # # p data.inspect
    if @insert_mode
      apply_insert(data)
      @insert_mode = false
    end

    if @update_mode
      apply_update(data)
      @update_mode = false
    end

    p "Before Delete"
    p data.inspect

    if @delete_mode
      apply_delete(data)
      @delete_mode = false
    end

    p "After Delete"
    p data.inspect

   return data

    #p data.inspect
  end
  

  private
end
  

  

request = MySqliteRequest.new

# This is to run the general query requests.

#     request.from('database.csv')
#            .select('lastname')
# #             .join('ID', 'database.csv', 'ID')
#              #.where('firstname', 'Thomas')
# #             .order('firstname', 'DESC')
#       results = request.run

#This is to insert new values into the CSV file

#  request.insert('database.csv')
#         .values(['4', 'Jeff', 'Bridges', '75', 'Winchester'])
#         results = request.run

#This is to update the values in the CSV files

# request.update('database.csv')
#     request.set('firstname', 'John')
#      .set('lastname', 'Canter')
#     .where('ID', '0')
#      results = request.run
 
#This is to delete values from the CSV files

# request.from('database.csv')
#         .delete("database.csv")
#         .where('ID', '2')
#         results = request.run


     results.each do |row|
      #puts row.inspect
    end