require 'eventmachine'
require 'json'

require 'follower_fetcher/queue'
require 'follower_fetcher/worker'

CREDENTIALS = JSON.parse File.read('./credentials.json')

EM.run do
  q = FollowerFetcher::Queue.new ENV['BROKER_URL']
  w = FollowerFetcher::Worker.new
  q.start(w)
  w.credentials = CREDENTIALS[(ENV['FETCHER_ID'].to_i || 0)]
  w.start
end