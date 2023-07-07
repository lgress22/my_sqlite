require 'readline'
require 'csv'
require_relative 'my_sqlite_request'

class MyCSVCLI
  def initialize
    @request = MySqliteRequest.new
  end

  def run
    puts "Welcome to MyCSV CLI!"

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
      file_name = args[0]
      @request.from(file_name)
      puts "FROM: #{file_name}"
    when 'select'
      columns = args
      @request.select(*columns)
      puts "SELECT: #{columns.join(', ')}"
    when 'where'
      column_name, criteria = args
      @request.where(column_name, criteria)
      puts "WHERE: #{column_name} = '#{criteria}'"
    when 'order'
      order, column_name = args
      @request.order(order, column_name)
      puts "ORDER: #{column_name} #{order}"
    when 'run'
      result = @request.run
      puts "Query executed. Result: #{result}"
    else
      puts "Invalid command: #{


 

# Create an instance of MyCSVCLI and run the CLI
 cli = MyCSVCLI.new
 cli.run
