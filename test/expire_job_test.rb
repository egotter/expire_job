require "test_helper"

class ExpireJobTest < Minitest::Test

  class Worker
  end

  class ExpireWorker
    def expire_in(*args)
      10
    end
  end

  def test_worker_without_expiry
    msg = {'args' => 'args', 'created_at' => Time.now.to_f}
    result = ExpireJob::Middleware.new.call(Worker.new, msg) { 'result' }
    assert result == 'result'
  end

  def test_worker_with_expiry
    msg = {'args' => 'args', 'created_at' => Time.now.to_f}
    result = ExpireJob::Middleware.new.call(ExpireWorker.new, msg) { 'result' }
    assert result == 'result'
  end
end
