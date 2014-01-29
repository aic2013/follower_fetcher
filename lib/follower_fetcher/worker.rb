require 'neography'
require 'redis'
require 'twitter'

module FollowerFetcher
  class Worker
    attr_accessor :queue

    attr_reader :is_running

    def initialize
      @is_running = false

      # Connect to Redis
      @redis = Redis.new
      puts "Connected to Redis: #{@redis.client.id}"

      # Connect to Neo4j
      @neo4j = Neography::Rest.new server: ENV['NEO4J_SERVER']
      puts "Connected to Neo4j: #{@neo4j.connection.server}"
    end

    def start
      return unless @queue && @client

      puts "Starting the worker..."

      queue.inbox_queue.subscribe(ack: true) do |delivery_info, metadata, payload|
        begin
          user = JSON.parse(payload)['user']
          process_user user['id'] unless user['protected']
          queue.channel.acknowledge(delivery_info.delivery_tag, false)
          # puts "User Done."
        rescue StandardError => error
          puts "Error while processing...reject message"
          queue.channel.reject(delivery_info.delivery_tag, true)
        end
      end

      @is_running = true
    end

    def stop
      @client.stop if @client
      @is_running = false
    end

    def status
      {
        name: 'follower-fetcher',
        running: self.is_running
      }
    end

    def credentials=(credentials)
      @credentials = credentials

      configure_twitter_client
    end

    private

      def configure_twitter_client
        was_running = self.is_running
        # Stop the worker
        stop

        @client = Twitter::REST::Client.new do |config|
          config.consumer_key        = @credentials['consumer_key']
          config.consumer_secret     = @credentials['consumer_secret']
          config.access_token        = @credentials['oauth_token']
          config.access_token_secret = @credentials['oauth_token_secret']
        end

        # Restart if it was running
        self.start if was_running
      end

      def process_user(user_id)
        begin
          if @redis.sadd('twitter_fetched_users', user_id)
            # puts "Processing User #{user_id}..."

            # Add user to Graph, if not already there
            batch = [ [cypher_merge_person(user_id)] ]

            # puts "Processing Followers #{user_id}..."

            # Fetch followers from Twitter
            followers = fetch_followers_from_twitter user_id
            batch << followers.map { |f| cypher_merge_follower(user_id, f) }

            # puts "Processing Friends #{user_id}..."

            # Fetch friends from Twitter
            friends = fetch_friends_from_twitter user_id
            batch << friends.map { |f| cypher_merge_follower(f, user_id) }

            queries = batch.flatten(1)
            puts "Excecuting query...(batch size: #{queries.size}, #{(queries.size / 10000).floor + 1} batches)"

            queries.each_slice(10000) do |slice|
              @neo4j.batch *slice
            end
          else
            puts "Duplicate user...Skipping."
          end
        rescue Twitter::Error => error
          puts "[INFO] Twitter error. Skipping."
        rescue Neography::NeographyError => error
          puts "[WARNING] #{error.code}: #{error.message}"
          @redis.srem('twitter_fetched_users', user_id)
          raise error
        rescue StandardError => error
          puts "[ERROR] #{error.code}: #{error.message}"
          @redis.srem('twitter_fetched_users', user_id)
          raise error
        end
      end

      def cypher_merge_person(user)
        [:execute_query, "MERGE (u:Person { id: #{user} })"]
      end

      def cypher_merge_follower(user, follower)
        [:execute_query, "MATCH (u:Person { id: #{user} }) MERGE (f:Person { id: #{follower} }) MERGE (f)-[:FOLLOWS]->(u)"]
      end

      ##
      # Twitter specific methods
      #
      def fetch_followers_from_twitter(user_id)
        fetch_with_retry { @client.follower_ids(user_id.to_i) }
      end

      def fetch_friends_from_twitter(user_id)
        fetch_with_retry { @client.friend_ids(user_id.to_i) }
      end

      MAX_REQUEST_RETRIES = 5
      def fetch_with_retry(&block)
        num_attempts = 0
        begin
          num_attempts += 1
          return yield
        rescue Twitter::Error::TooManyRequests => error
          if num_attempts <= MAX_REQUEST_RETRIES
            puts "[INFO] Rate limit sleeping for #{error.rate_limit.reset_in}"
            sleep error.rate_limit.reset_in
            retry
          else
            raise
          end
        rescue Twitter::Error::NotFound => error
          raise error
        rescue Twitter::Error => error
          puts "[ERROR] #{error.code}: #{error.cause} (Limit remaining: #{error.rate_limit.remaining})"
          raise error
        end
      end

      ##
      # NEO4J specific methods
      #

      def create_distinct_user(user_id)
        begin
          users = @neo4j.find_nodes_labeled("Person", id: user_id)
          user = users.first
          if users.empty?
            user = @neo4j.create_node(id: user_id)
            @neo4j.add_label(user, "Person")
          end
          user
        rescue Neography::NeographyError => error
          puts "[ERROR] #{error.code}: #{error.message}"
          raise
        end
      end

      def make_follower_connection(from, to)
        @neo4j.create_relationship("FOLLOWS", from, to)
      end
  end
end