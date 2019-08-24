#!/usr/bin/env ruby

# file: sqlite_server2018_plus.rb

# description: Designed to provide fault tolerant access to a DRb SQLite server
#              when 2 or more back-end nodes are running.

require 'drb'
require 'socket'

class SQLiteServer2018PlusException < Exception
end

class SQLiteServer2018
  
  attr_accessor :nodes

  def initialize(nodes, debug: debug)
    
    @nodes, @debug = nodes, debug
    

    if (nodes & Socket.ip_address_list.map(&:ip_address)).any? then
      raise SQLiteServer2018PlusException, 
          'Cannot use host IP address in node list'
    end
    
    @failcount = 0
    @db = fetch_server(nodes.first)
    
  end    
  
  def execute(dbfile, *args, &blk)
    
    puts 'inside SQLiteServer2018::execute args: ' + args.inspect if @debug
    
    if block_given? then
      a = db_op { @db.execute(dbfile, *args)  }
      a.each(&blk)
    else
      db_op { @db.execute dbfile, *args, &blk }
    end

  end
  
  def exists?(dbfile)
    
    puts 'inside SQLiteServer2018::exists?' if @debug
    db_op { @db.exists? dbfile }
        
  end
  
  def fields(*args)
    db_op { @db.fields *args }
  end  
  
  def load_db(dbfile)    
        
    puts 'inside SQLiteServer2018::load' if @debug
    db_op { @db.load_db dbfile }
    
  end
  
  def ping()
    db_op { @db.ping }
  end  
  
  def query(*args, &blk)
    
    puts 'inside SQLiteServer2018::query args: ' + args.inspect if @debug
    db_op { @db.query *args, &blk }
    
  end  
  
  def results_as_hash(*args)    
    db_op { @db.results_as_hash *args }
  end
  
  def results_as_hash=(*args)
    db_op { @db.results_as_hash = *args }
  end    
  
  def table_info(*args)
    db_op { @db.table_info *args }
  end
  
  def tables(dbfile)    
    db_op { @db.tables dbfile }        
  end    


  private

  def db_op()

    begin      
      r = yield()
      @failcount = 0
      r
    rescue
      puts 'warning: ' + ($!).inspect
      
      if @debug then
        puts '@nodes: ' + @nodes.inspect 
        puts '@failcount: ' + @failcount.inspect
      end
      
      @nodes.rotate!
      @db = fetch_server(@nodes.first)
      @failcount += 1
      retry unless @failcount > @nodes.length
      raise 'SQLiteServer2018Plus nodes exhausted'
      exit
    end

  end
  
  def fetch_server(host)
    port = '57000'
    DRbObject.new nil, "druby://#{host}:#{port}"    
  end

end

class SQLiteServer2018Plus


  def initialize(host: 'localhost', port: '57000', nodes: [], debug: false)

    @host, @port, @nodes, @debug = host, port, nodes, debug

  end

  def start()
    
    DRb.start_service "druby://#{@host}:#{@port}", 
        SQLiteServer2018.new(@nodes, debug: @debug)
    DRb.thread.join

  end

end
