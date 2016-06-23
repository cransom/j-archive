#!/usr/bin/env ruby
require 'thread'
require 'ftpd'
require 'tmpdir'
require 'yaml'
require 'git'
require 'pry'
require 'zlib'
require 'stringio'
require 'elasticsearch'

@config = {}


log = Logger.new($stdout)

class Driver
  attr_accessor :user

  def initialize(temp_dir,git_directory)
    @temp_dir = temp_dir
    @git_directory = git_directory
  end
  def authenticate(user, password)
    @user == user
  end

  def file_system(user)
    GitArchiveFileSystem.new(@temp_dir,@git_directory)
  end
end

class GitArchiveFileSystem
  include Ftpd::DiskFileSystem::Base
  @@git_lock = Mutex.new
  log = Logger.new($stdout)
  def initialize(temp_dir,git_directory)
    set_data_dir temp_dir
    begin
      @repo = Git.open(git_directory)
    rescue ArgumentError
      @repo = Git.init(git_directory)
    end
  end
  def accessible?(ftp_path)
    true
  end
  def directory?(ftp_path)
    true
  end
  def exists?(ftp_path)
    true
  end
  def write(ftp_path, contents)
    if /binary/.match(ftp_path)
      File.open(@repo.dir.to_s + ftp_path,'w') { |f| f.write contents }
    else
      file_name = ftp_path.split("/").last
      host_name = file_name.split("_").first
      dest_name = @repo.dir.to_s + "/" + host_name 
      dest_name += ".conf" unless  dest_name =~ /.conf/

      #At this point, should make a call to logstash/elasticsearch to try and find
      #who made this commit. 

      File.open(dest_name,'w') { |f| f.write decompress(contents) }
      @@git_lock.synchronize {
        @repo.add(dest_name)
        begin
          #unless @repo.status.changed.count == 0
          @repo.commit('no commit info for ' + host_name)
        rescue Git::GitExecuteError => git_error
          if git_error.message.match("working directory clean")
            puts "nothing to do"
          else
            throw git_error
          end
        end
      }
    end
  end
  def decompress(data)
    if data.bytes.to_a[0,2] == [31, 139]
      #log.debug("decompressing gzip file")
      Zlib::GzipReader.new(StringIO.new(data.to_s)).read.to_s
    else
      #log.debug("clear text")
      #binding.pry
      data
    end
  end
end

def load_config(file)
  @config = YAML::load(File.open(file))
end

load_config(File.expand_path(File.dirname(__FILE__)) + "/config.yml")

File.open(@config[:pidfile],"w") { |f| f.write($$) }

Dir.mktmpdir do |temp_dir|
  driver = Driver.new(temp_dir,@config[:git_directory])
  driver.user = @config[:ftp_user]
  server = Ftpd::FtpServer.new(driver)
  server.port = @config[:port]
  server.auth_level = Ftpd::AUTH_USER
  server.allow_low_data_ports = false
  server.interface = @config[:listen_address]
  server.log = log
  begin
    server.start
  rescue Errno::ETIMEDOUT => timeout
    retry
  end
  puts "Server listening on port #{server.bound_port}"
end

#Block to prevent exit
$stdin.gets
#Thread.list.each{|t| t.join }



