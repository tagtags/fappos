#!/usr/bin/env ruby
require 'json'
require 'sqlite3'
require 'securerandom'
require 'fileutils'

def db_location; './db' ;end
def chunk_size;  1000   ;end

def running(pid)
	begin
  	Process.getpgid( pid )
  	true
	rescue Errno::ESRCH
	  false
	end
end

class Lgr
	def self.info(s)
		puts s
	end
end

class ScanDB
	def initialize( parms, drive, drive_info, relative_path, apath )


		FileUtils.mkpath db_location

		db_name = "#{db_location}/#{SecureRandom.hex}.db"

		@db = SQLite3::Database.new(db_name)

		rows = @db.execute <<-SQL 

CREATE TABLE scan_data(
 filename 			varchar(255),
 filepath 			varchar(4096),
 relative_path  varchar(2),
 folder   			int,
 file_size      int,
 file_count     int,
 mod_date       varchar(10),
 mod_time       varchar(19),
 mod_offset     varchar(5)
);

SQL

		rows = @db.execute <<-SQL 

CREATE TABLE metadata(
  mkey          varchar(4096),
  mvalue        text
);

SQL

		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'parms_a',       parms.to_json      ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'drive',         drive              ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'relative_path', relative_path      ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'drive_info_h',  drive_info.to_json ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'spath',         apath.join('/')    ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'pid',           Process.pid        ])
		rows = @db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'scan_start',    Time.now.to_s      ])
# scan_end       varchar(25),
# tot_count      int,
# tot_size       int

	end

	def sync_on
		@db.execute('PRAGMA synchronous=ON;')
	end

	def sync_off
		@db.execute('PRAGMA synchronous=OFF;')
	end

	def add( filename, filepath, folder, relative_path, file_size, file_count, mod_date, mod_time, mod_offset )
		retries = 100
		begin
			@db.execute("INSERT INTO scan_data( filename, filepath, folder, relative_path, file_size, file_count, mod_date, mod_time, mod_offset ) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ? )",[ filename, filepath, folder, relative_path, file_size, file_count, mod_date, mod_time, mod_offset ])
		rescue SQLite3::BusyException
	  	retries -= 1
	  	if retries > 0
	  		puts "db retries #{retries}"
	  		sleep(1.0/(rand(5)+1))
	  		retry
	  	end
	  rescue Exception => e
	  	puts e.class.name 
	  	puts e.message
	  	puts e.backtrace
	  end
	end
end

class DriveInfo

	def initialize( parms )
		@parms = parms
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
						rescue Exception => e
							#puts e.message
							#puts e.backtrace
							usage
							exit
						end
					end

					if parms[:args].include?('-s')
						begin
							drive = parms[:args][ parms[:args].find_index('-s')+1 ]
							drive_name = drive_from_num(drive)
							existing_scans = scans_matching_drive(drive_name)
							if existing_scans 
								if parms[:args].include?('-f')  # force
									delete_scans(existing_scans)
									scan_local_drive( drive )
								else
									puts "Scans existing for #{drive_name}"
									puts " use force option (-f) to overwrite"
									puts
								end
							else
								scan_local_drive( drive )
							end
						rescue Exception => e
							puts e.message
							puts e.backtrace
							usage
							exit
						end
					end

					if parms[:args].include?('-ls')
						begin
							each_scan do |scan|
								pid = 0
								if scan.has_key?('pid')
									pid = scan['pid'].to_i
									if pid>0
										if running(pid)
											pid = "#{pid} running"
										else
											if scan.has_key?('scan_end')
												pid = "finished"
											else
												pid = "#{pid} interrupted"
											end
										end
									end
								end
								if scan.has_key?('scan_end')
									puts "#{scan['db_filename']} #{scan['drive']} #{pid} #{scan['scan_end']} #{scan['tot_count']} #{scan['tot_size']}"
								else
									puts "#{scan['db_filename']} #{scan['drive']} #{pid} #{scan['scan_start']}"
								end
							end
						rescue Exception => e
							puts e.message
							puts e.backtrace
							usage
							exit
						end
					end

				end
			end
		end

	end

	def scans_matching_drive(drive)
		scans = []
		each_scan do |scan|
			scans << scan if scan['drive'] == drive
		end
		if scans.length == 0
			return nil
		end
		return scans
	end

	def delete_scans(scans)
		scans.each do |scan|
			fname = scan['db_location']+'/'+scan['db_filename']
			puts "Deleting #{fname}"
			File.delete(fname)
		end
	end

	def each_scan
		Dir.glob("#{db_location}/*.db") do |db_file|
			retries = 10
			begin
	  		md = {}
	  		md['db_location'] = db_location
	  		md['db_filename'] = File.basename(db_file)
	  		db = SQLite3::Database.open(db_file)
	  		db.execute("SELECT * FROM metadata;").each do |row|
	  			#puts "row = #{row.inspect}"
	  			suff = row[0][-2..-1]
	  			if '_h'==suff || '_a'==suff
	  				md[row[0]] = JSON.parse(row[1])
	  			else
	  				md[row[0]] = row[1]
	  			end
	  		end
	  		yield md
	  	rescue SQLite3::BusyException
	  		retries -= 1
	  		if retries > 0
	  			sleep(1.0/(rand(5)+1))
	  			retry
	  		end
	  	rescue Exception => e
	  		puts e.class.name 
	  		puts e.message
	  		puts e.backtrace
	  	end
		end
	end

	def usage
		puts
		puts "Usage: drive_scan.rb <options>"
		puts "   -l                          list local drives"
		puts "   -ms  <drive | drive index>  Mark drive for Scanning"
		puts "   -s   <drive>                Scan drive"
		puts "   -sa                         Scan all drives"
		puts "   -ls                         List scans"
		puts "   -f                          force"
		puts "   -ws                         web server"
		puts "   -p <port#>                  web server port number"
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

	FILE_PERMISSIONS  = 0
	NUMBER_OF_LINKS   = 1
	OWNER_NAME        = 2
	OWNER_GROUP       = 3
	FILE_SIZE         = 4
	LAST_MOD_DATE     = 5
	LAST_MOD_TIME     = 6
	LAST_MOD_OFFSET   = 7
	FILENAME          = 8

	REGULAR_FILE  = '-'
	DIRECTORY     = 'd'
	SYMBOLIC_LINK = 'l'
	NAMED_PIPE    = 'p'
	SOCKET        = 's'
	CHAR_DEVICE   = 'c'
	BLOCK_DEVICE  = 'b'
	DOOR          = 'D'

	def recurse_scan( db, relative_path, apath )
		tot_size       = 0
		tot_file_count = 0
		#puts "scan_data: #{scan_data} \nrelative_path: #{relative_path} \napath: #{apath}"
		fpath = "#{relative_path}#{apath.join('/')}".gsub("'","'\\\\''")
		#if fpath.include?('Fox')
		#	puts "#{fpath}"
		#	puts apath.to_s
		#end
		file_list = `ls '#{fpath}' --time-style=full-iso -lArt 2>&1` ;  result=$?.success?
		skip = false
		unless result
			puts "error file list = #{file_list}"
			puts "apath = #{apath.to_s}"
			puts "fpath = #{fpath}"
 			xpath = "#{relative_path}#{apath.join('/')}"
 			puts "xpath = #{xpath}"
 			xspath = xpath.gsub("'","'\\\\''")
 			puts "xspath.gsub = #{xspath}"
			raise "failed ls" unless file_list.include?("Permission denied")
			skip = true
		end
		#puts file_list
		unless skip
			fla = []
			file_list.each_line do |line|
				lsa = line.split
				ftime = "#{lsa[LAST_MOD_TIME]} #{lsa[LAST_MOD_OFFSET]}"
				#puts "ftime = #{ftime}"
				pos = line.index(ftime) + ftime.length + 1
				lsa.pop while lsa.length>8
				lsa[FILENAME] = line[pos..-1].strip
				fla << lsa
			end
			fla.shift
			fla.each do |a|
				size           = 0
				file_count     = 0
	#puts "a.to_s = #{a.to_s}"
				if DIRECTORY == a[FILE_PERMISSIONS][0]
					b = apath.clone
					b << a[FILENAME]
					size, file_count = recurse_scan( db, relative_path, b )
					tot_size       += size
					tot_file_count += file_count
					#puts "#{'%-12.12s' % size.to_s} #{'%-6.6s' % file_count.to_s} #{relative_path}#{apath.join('/')}/#{a[FILENAME]}"
					db.add( a[FILENAME], apath.join('/'), 1, relative_path, size, file_count, a[LAST_MOD_DATE], a[LAST_MOD_TIME], a[LAST_MOD_OFFSET] )
				else
					file_count = 1
					size = a[FILE_SIZE].to_i
					tot_size       += size
					tot_file_count += file_count
					#puts "#{'%-19.19s' % size.to_s} #{relative_path}#{apath.join('/')}/#{a[FILENAME]}"
					db.add( a[FILENAME], apath.join('/'), 0, relative_path, size, file_count, a[LAST_MOD_DATE], a[LAST_MOD_TIME], a[LAST_MOD_OFFSET] )
				end
			end
		end
		return tot_size, tot_file_count
	end

	def mark_local_drive(drive,mark)
		skip = true
		if @drive_info['local-drive-list'].include?(drive)
			skip = false
		else
			drive_index = drive.to_i
			puts "mark_index #{drive_index}"
			if drive_index.to_s == drive
				if drive_index < @drive_info['local-drive-list'].length
					drive = @drive_info['local-drive-list'][drive_index]
					skip = false
				end
			end
		end
		unless skip
			@drive_info['local-drive-options'] ||= {}
			@drive_info['local-drive-options'][drive] ||= {}
			@drive_info['local-drive-options'][drive]['_ary'] ||= []
			@drive_info['local-drive-options'][drive]['_ary'] << mark
			save 
		end
	end

	def drive_from_num(drive)
		return drive if @drive_info['local-drive-list'].include?(drive)
		drive_index = drive.to_i
		if drive_index.to_s == drive
			if drive_index < @drive_info['local-drive-list'].length
				return @drive_info['local-drive-list'][drive_index]
			else 
				return nil
			end
		else
			return nil
		end
	end

	def scan_local_drive(drive)
		skip = true
		if @drive_info['local-drive-list'].include?(drive)
			skip = false
		else
			drive_index = drive.to_i
			puts "mark_index #{drive_index}"
			if drive_index.to_s == drive
				if drive_index < @drive_info['local-drive-list'].length
					drive = @drive_info['local-drive-list'][drive_index]
					skip = false
				end
			end
		end
		if skip
			raise "drive #{drive} not found in drive list (-l)"
		else
			outfile = "#{drive.gsub('/','__')}.drivescan.json"
			puts "outfile: #{outfile}"
			
			scan_data = {}
			sdrive = drive.strip

			relative_path = ''
			if sdrive[-1] == '/'
				sdrive = sdrive[0..sdrive.length-2]
			end
			if sdrive[0] == '/'
				sdrive = sdrive[1..sdrive.length-1]
				relative_path = '/'
			end
			puts "sdrive: #{sdrive}"
			apath = sdrive.split('/')

			db = ScanDB.new(@parms,drive,@drive_info,relative_path,apath)
			db.sync_off
			tot_size,tot_count = recurse_scan(db,relative_path,apath)
			db.sync_on
			rows = db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'scan_end',  Time.now.to_s ])
			rows = db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'tot_count', tot_count     ])
			rows = db.execute( "INSERT INTO metadata( mkey, mvalue) VALUES (?,?)",[ 'tot_size',  tot_size      ])

		end
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


