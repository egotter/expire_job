require 'time'
require 'logger'

require "expire_job/version"

module ExpireJob
  class Middleware
    def call(worker, msg)
      if worker.respond_to?(:expire_in)
        picked_time = pick_enqueued_at(msg)
        parsed_time = parse_time(picked_time)
        if perform_expire_check(worker, msg['args'], worker.expire_in, parsed_time)
          yield
        end
      else
        yield
      end
    end

    def perform_expire_check(worker, args, expire_in, enqueued_at)
      if enqueued_at.nil?
        logger.warn { "Can not expire this job because enqueued_at is not found. args=#{truncate(args.inspect)}" }
        return true
      end

      if enqueued_at < Time.now - expire_in
        logger.info { "Skip expired job. args=#{truncate(args.inspect)}" }

        perform_callback(worker, :after_expire, args)

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
        logger.info { "enqueued_at was found in args. enqueued_at=#{enqueued_at}" } if enqueued_at
      end

      if enqueued_at.nil?
        # The msg has both created_at and enqueued_at.
        #   created_at: is a time when #perform_async or #perform_in is called
        #   enqueued_at: is a time when the job is inserted into a queue
        enqueued_at = msg['created_at'] # TODO Use enqueued_at?
        logger.debug { "enqueued_at was found in msg. enqueued_at=#{enqueued_at}" } if enqueued_at
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
      logger.warn { "Can not parse this value. value=#{value.inspect}" }
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
          message = "The number of parameters of the callback method (#{parameters.size}) is not the same as the number of arguments (#{args.size})"
          raise ArgumentError.new("#{self.class}:#{worker.class} #{message} callback_name=#{callback_name} args=#{args.inspect} parameters=#{parameters.inspect}")
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
