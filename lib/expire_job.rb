require 'time'
require 'logger'

require "expire_job/version"

module ExpireJob
  class Middleware
    def call(worker, msg, queue, &block)
      if worker.respond_to?(:expire_in)
        picked_time = pick_enqueued_at(msg)
        parsed_time = parse_time(picked_time)

        if perform_expire_check(worker.expire_in, parsed_time)
          yield
        else
          logger.info { "[ExpireJob] Expired job is skipped. args=#{truncate(msg['args'].inspect)}" }
          perform_callback(worker, :after_expire, msg['args'])
          nil
        end
      else
        yield
      end
    end

    def perform_expire_check(expire_in, enqueued_at)
      if enqueued_at.nil?
        logger.warn { "[ExpireJob] Can not expire this job because enqueued_at is nil." }
        return true
      end

      if enqueued_at < Time.now - expire_in
        false
      else
        true
      end
    end

    def pick_enqueued_at(msg)
      args = msg['args']
      enqueued_at = nil

      if args.is_a?(Array) && args.size >= 1 && args.last.is_a?(Hash)
        enqueued_at = args.last['enqueued_at']
        logger.info { "[ExpireJob] enqueued_at was found in args. enqueued_at=#{enqueued_at}" } if enqueued_at
      end

      if enqueued_at.nil?
        # The msg has both created_at and enqueued_at.
        #   created_at: is a time when #perform_async or #perform_in is called
        #   enqueued_at: is a time when the job is inserted into a queue
        enqueued_at = msg['created_at'] # TODO Use enqueued_at?
        logger.debug { "[ExpireJob] enqueued_at was found in msg. enqueued_at=#{enqueued_at}" } if enqueued_at
      end

      enqueued_at
    end

    def parse_time(value)
      if value.to_s.match?(/\d+\.\d+/)
        Time.at(value)
      else
        Time.parse(value)
      end
    rescue => e
      logger.warn { "[ExpireJob] Can not parse this value. value=#{value.inspect}" }
      nil
    end

    def perform_callback(worker, callback_name, args)
      if worker.respond_to?(callback_name)
        parameters = worker.method(callback_name).parameters

        begin
          if parameters.empty?
            worker.send(callback_name)
          else
            worker.send(callback_name, *args)
          end
        rescue ArgumentError => e
          raise ArgumentError.new("[ExpireJob] Invalid parameters callback_name=#{callback_name}")
        end
      end
    end

    def truncate(text, length: 100)
      if text.length > length
        text.slice(0, length)
      else
        text
      end
    end

    def logger
      if defined?(::Sidekiq)
        ::Sidekiq.logger
      elsif defined?(::Rails)
        ::Rails.logger
      else
        ::Logger.new(STDOUT)
      end
    end
  end
end
