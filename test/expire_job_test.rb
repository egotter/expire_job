require "test_helper"

class ExpireJobTest < Minitest::Test

  class Worker
  end

  class ExpireWorker
    def expire_in(*args)
      10
    end

    def after_expire(*args)
      'callback'
    end
  end

  def test_call
    msg = {'args' => 'args', 'created_at' => Time.now.to_f}
    result = ExpireJob::Middleware.new.call(Worker.new, msg) { 'result' }
    assert result == 'result'

    msg = {'args' => 'args', 'created_at' => Time.now.to_f}
    result = ExpireJob::Middleware.new.call(ExpireWorker.new, msg) { 'result' }
    assert result == 'result'
  end

  def test_perform_expire_check
    expire_in = ExpireWorker.new.expire_in

    enqueued_at = nil
    result = ExpireJob::Middleware.new.perform_expire_check(expire_in, enqueued_at)
    assert result

    enqueued_at = Time.now
    result = ExpireJob::Middleware.new.perform_expire_check(expire_in, enqueued_at)
    assert result

    enqueued_at = Time.at(Time.now.to_i - (expire_in + 1))
    result = ExpireJob::Middleware.new.perform_expire_check(expire_in, enqueued_at)
    assert !result
  end

  def test_pick_enqueued_at
    msg = {}
    result = ExpireJob::Middleware.new.pick_enqueued_at(msg)
    assert result.nil?

    msg = {'args' => [{'enqueued_at' => 'time'}]}
    result = ExpireJob::Middleware.new.pick_enqueued_at(msg)
    assert result == 'time'

    msg = {'created_at' => 'time'}
    result = ExpireJob::Middleware.new.pick_enqueued_at(msg)
    assert result == 'time'
  end

  def test_parse_time
    time = Time.now
    result = ExpireJob::Middleware.new.parse_time(time.to_f)
    assert result.to_i == time.to_i

    time = Time.now
    result = ExpireJob::Middleware.new.parse_time(time.to_s)
    assert result.to_i == time.to_i
  end

  def test_perform_callback
    args = 'args'
    result = ExpireJob::Middleware.new.perform_callback(Worker.new, :after_expire, args)
    assert result.nil?

    args = 'args'
    result = ExpireJob::Middleware.new.perform_callback(ExpireWorker.new, :after_expire, args)
    assert result == 'callback'
  end

  def test_truncate
    result = ExpireJob::Middleware.new.truncate('a' * 10)
    assert result == 'a' * 10

    result = ExpireJob::Middleware.new.truncate('a' * 200)
    assert result == 'a' * 100
  end

  def test_logger
  end
end
