require 'bunny'
require 'json'

module FollowerFetcher
  TARGET_NAME = 'follower-fetcher'

  class Queue
    attr_reader :inbox_queue, :channel

    def initialize(url = 'amqp://guest:guest@localhost:5672/')
      @amqp_url = url
    end

    def start(worker)
      return unless worker

      @worker = worker
      @worker.queue = self

      # Create connection
      puts "Connecting to RabbitMQ..."
      @connection = Bunny.new @amqp_url
      @connection.start

      # Create a channel
      @channel = @connection.create_channel

      # Create a queue and an exchange
      @inbox_queue = @channel.queue('follower-fetcher', durable: true)
      @control_queue = @channel.queue('control')
      @channel.queue('control.reply')
      @exchange = @channel.default_exchange
      @channel.prefetch(1)

      # Listen on control queue
      @control_queue.subscribe do |delivery_info, metadata, payload|
        target = metadata[:headers]['target']
        if target == TARGET_NAME || target == 'all'
          process_command(metadata[:headers]['command'], payload)
        end
      end
    end

    def stop
      puts "Closing connection to RabbitMQ..."
      @connection.close if @connection
    end

    private

      def process_command(command, payload)
        return unless @worker

        case command
        when 'start'
          puts "Starting worker..."
          @worker.start
        when 'stop'
          puts "Stopping worker..."
          @worker.stop
        when 'status'
          @exchange.publish(@worker.status.to_json, routing_key: 'control.reply', content_type: 'application/json')
        when 'credentials'
          puts "Setting credentials..."
          @worker.credentials = JSON.parse payload
        end
      end

  end
end