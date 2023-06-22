require 'readline'
require_relative 'my_sqlite_request'

class MySqliteCLI
  def initialize
    @request = MySqliteRequest.new
  end

  def run
    puts "Welcome to MySqlite CLI!"

    loop do
      line = Readline.readline('> ', true)
      break if line.nil? || line.strip.downcase == 'exit'

      process_command(line)
    end

    puts "Goodbye!"
  end

  def process_command(line)
    command, *args = line.split
    case command
    when 'from'
      table_name = args[0]
      @request.from(table_name)
      puts "FROM: #{table_name}"
    when 'select'
      columns = args
      @request.select(*columns)
      puts "SELECT: #{columns.join(', ')}"
    when 'where'
      column_name, criteria = args
      @request.where(column_name, criteria)
      puts "WHERE: #{column_name} = '#{criteria}'"
    when 'join'
      column_a, filename_b, column_b = args
      @request.join(column_a, filename_b, column_b)
      puts "JOIN: #{column_a} = #{filename_b}.#{column_b}"
    when 'order'
      order, column_name = args
      @request.order(order, column_name)
      puts "ORDER: #{column_name} #{order}"
    when 'run'
      result = @request.run
      puts "Query executed. Result: #{result}"
    else
      puts "Invalid command: #{command}"
    end
  end
end

cli = MySqliteCLI.new
cli.run
