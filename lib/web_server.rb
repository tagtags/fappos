#web_server.rb
require 'sinatra/base'
require './lib/scans.rb'

class WebServer < Sinatra::Base
	get '/' do 
		@scans ||= Scans.new
		erb :index
	end
end