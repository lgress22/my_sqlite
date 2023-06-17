require 'sqlite3'
require 'csv'


class MySqliteRequest
    def initialize

        file_path = 'C:\Users\lgres\Downloads\nba_player_data.csv'
        CSV.foreach(file_path) do |row|
            puts row.inspect
        end

        CSV.read("nba_player_data.csv")
        @table_name = nil;
        @select_columns = [];
        @where_conditions = [];
        @join_table = nil;
        @join_condition = nil;
        @query = {};
        @query_result = nil;
    end

    def from(table_name)
        @table_name = CSV.read('nba_player_data.csv', headers: true);
        self;
    end

    def select(*column_name)
        @select_columns = column_name.flatten;
        self;
    end

    def where(column_name, criteria)
        condition = "#{column_name} = '#{criteria}'";
        @where_condition << condition;
        self;
    end

    def join(column_a, filename_b, column_b)
        @join_table = filename_b;
        @join_condition = "#{@table_name}.#{column_a} = #{@join_table}.#{column_b}";
    end

    def order(order, column_name)
        @order = "#{column_name} #{order}";
        self;
    end

    def insert(table_name)
        @insert_table = table_name
        self
    end

    def self.insert(table_name)
        request = self.new
        request.insert(table_name)
        request
      end    

    def values(data)
        @insert_values =  data;
        self;
    end

    def update(table_name)
        @update_table = table_name;
        self;
    end

    def set(data)
        @update_values = data;
        self;
    end

    def run
        
    end
end