#!/usr/bin/env ruby
require 'json'


class Lgr
	def self.info(s)
		puts s
	end
end


class DriveInfo

	def initialize( parms )
		Lgr.info "Initializing DriveInfo..."
		@filename = "file_systems.json"
		if File.file?(@filename)
			filedata = File.read(@filename)
			@drive_info = JSON.parse(filedata)
		else
			@drive_info = {}
			save
	  end
	  update
		Lgr.info "Initialization Complete..."

		if parms
			if parms.is_a?(Hash)
				if parms.has_key?(:args)
					if parms[:args].include?('-?')
						usage
						exit
					end
					if parms[:args].include?('-l')

						if @drive_info.has_key?('local-drive-options')
							max_length = 0
							@drive_info['local-drive-list'].each { |item| max_length = item.length if item.length>max_length }
							@drive_info['local-drive-list'].each_with_index do |item,index|
								line = index.to_s.ljust(10) + ' ' + item.ljust(max_length+3) 
								tags = @drive_info.dig( 'local-drive-options', item, '_ary' )
								line = line + ' ' + tags.join(',') if tags
								puts line
							end

						else
							@drive_info['local-drive-list'].each_with_index do |item,index|
								puts index.to_s.ljust(10) + ' ' + item
							end
						end

					end
					if parms[:args].include?('-ms')
						begin
							puts "index #{parms[:args].find_index('-ms')}"
							mark_local_drive( parms[:args][ parms[:args].find_index('-ms')+1 ], 'scan' )
						rescue
							usage
							exit
						end
					end

				end
			end
		end

	end

	def usage
		puts
		puts "Usage: drive_scan.rb <options>"
		puts "   -l   list local drives"
		puts "   -ms  <drive index|drive> Mark drive for Scanning"
		puts
	end

	def update
		changed = false
		# get drive list if absent
		unless @drive_info.has_key?('local-drive-list')
			drive_list = []
			df = `df | awk '{print $6}'`
			df.each_line do |line|
				drive_list << line.strip
			end
			if drive_list.length>0
				drive_list.shift
			end
			#puts drive_list
			@drive_info['local-drive-list'] = drive_list
			changed = true
		else

		end
		save if changed
	end

	def mark_local_drive(drive,mark)
		#@drive_info['local-drive-options'] = {} unless @drive_info.has_key?('local-drive_options')
		@drive_info['local-drive-options'] ||= {}
		@drive_info['local-drive-options'][drive] ||= {}
		@drive_info['local-drive-options'][drive]['_ary'] ||= []
		@drive_info['local-drive-options'][drive]['_ary'] << mark
		save
	end

	def save
		File.open(@filename,"w") do |f|
	 		f.write(@drive_info.to_json)
	 		Lgr.info "file data saved"
	 	end
	end

end


class DriveScanner

	def initialize(location)
		@drive = {}
		@drive['location'] = location
	end

	def scan

	end

	def self.list_drives
	end

end

if __FILE__==$0
  # this will only run if the script was the main, not load'd or require'd
  di = DriveInfo.new({:args => ARGV})
end


