# frozen_string_literal: true

require 'mongo'
require 'faker'
require 'slop'
require 'json'
require 'progress_bar'

opts = Slop.parse do |o|
  o.string '-h', '--host', 'the connection string for the MongoDB cluster (default: localhost)',
           default: 'mongodb://localhost'
  o.string '-d', '--database', 'the database to use (default: hs2)', default: 'sales'
  o.string '-c', '--collection', 'the collection to use (default: p1)', default: 'product_temp'
end

# Connect to the DB
# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "## Connecting to #{opts[:host]}, and db #{opts[:database]}\n\n"
DB = Mongo::Client.new(opts[:host], database: opts[:database])


# set the collection to use
products = DB[opts[:collection]]
#start a counter
i = 0



loop do # loop forever
  # Find a random product
  rando = products.aggregate([
                               { '$sample' => { 'size' => 1 } }
                             ]).to_a

  
                             # create a review using the name of the product
  reviewText = "#{[:"I was a little surprised with this", :"It's says in the description it's a",
                   :"I'm utterly horrified with this", :"Frankly i'm dumbfounded with this"].sample}"\
               " #{[Faker::Adjective.positive, Faker::Adjective.negative].sample} #{rando[0]['title']}."\
               " I can tell you with #{Faker::Emotion.adjective} #{Faker::Emotion.noun} that it is the #{[:best, :worst,
              :"most unpleasant", :"most funny", :"by far the most delightful little", :"most bewildering", :"most alarming"].sample} #{%i[
                thing item object
              ].sample}"\
               " #{[:"I've used in a long time!", :"I've had the pleasure of owning!", :"I've ever seen!",
                    :"I've had the misfortune to stumble upon"].sample} #{[:"- I love it!!", :"it's utter rubbish",
              :"I'm thinking about buying another one", :"I'll be returning it on tuesday", :"it was going to be a gift for my Mother"].sample}"

  
              # give it a random star rating
  stars = rand(0.0..5.0).round(1)

  # create a sub document called review
  review = {
    'content' => reviewText,
    'author' => [Faker::FunnyName.name, Faker::FunnyName.three_word_name, Faker::Name.name].sample.to_s,
    'stars' => stars,
    'ts' => Time.now
  }

  # push the newly created review document into the reviews array in the original random document we pulled out.
  doc = products.find_one_and_update({ _id: rando[0]['_id'] }, { '$push' => { reviews: review } },
                                     return_document: :after)
  puts "[#{i}] - #{doc[:_id]}"
  # increment the counter
  i += 1
end











# average - https://www.mongodb.com/docs/manual/reference/operator/aggregation/set/#creating-a-new-field-with-existing-fields
