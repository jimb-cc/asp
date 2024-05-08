# frozen_string_literal: true

require 'mongo'
require 'faker'
require 'slop'
require 'json'
require "awesome_print"

opts = Slop.parse do |o|
  o.string '-h', '--host', 'the connection string for the MongoDB cluster (default: localhost)',
           default: 'mongodb://localhost'
  o.string '-d', '--database', 'the database to use (default: hs2)', default: 'sales'
  o.string '-c', '--collection', 'the collection to use (default: p1)', default: 'product_temp'
end

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "## Connecting to DB: #{opts[:database]}\n"
DB = Mongo::Client.new(opts[:host], database: opts[:database])

# set the collection to use
coll = DB[opts[:collection]]






## Create a change stream, to watch the collection

#stream = coll.watch
#stream = coll.watch([{'$match' => { "updateDescription.updatedFields.reviews.0.stars" => { '$gte' => 4 } } }])
stream = coll.watch([{'$match' => { "updateDescription.updatedFields.reviews.0.author" => "Al Gore Rythim" }}])


#Get the watch cursor, while there is a next item on it, print it to screen 
enum = stream.to_enum
while doc = enum.next
  ap doc
  puts "\n\n\n\n\n\n"
end