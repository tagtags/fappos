#scans.rb

class Scans

	def db_location
		'./db'
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
	  		raise
	  	rescue Exception => e
	  		puts e.class.name 
	  		puts e.message
	  		puts e.backtrace
	  	end
		end
	end	

end