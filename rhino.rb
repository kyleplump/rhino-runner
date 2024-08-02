# frozen_string_literal: true

module Rhino
  require 'redis'
  require 'json'

  class Queue
    def initialize(name:)
      @name = name
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
    end

    def add(job_name:, data:)
      @redis.set("rhino:#{@name}:#{job_name}", data.to_json)
    end
  end

  class Worker
    def initialize(name:, handler:)
      @name = name
      @handler = handler
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
      Thread.new do
        check_queue # immediately process queue

        # this sucks
        loop do
          sleep 1
          check_queue
        end
      end
    end

    private

    def check_queue
      p 'queue check'
      res = @redis.keys("rhino:#{@name}:*")

      jobs = res.map do |key|
        data = @redis.get(key)

        Ractor.new [@handler, @redis, key, data] do |handler, redis, key, data|
          data_obj = JSON.parse(data)
          method(handler).call(key.split(':').last, data_obj)
          redis.del(key)
        end
      end

      jobs.each(&:take)
    end
  end
end

r = Rhino::Queue.new(name: 'tester')

10.times do |i|
  r.add(job_name: "job_#{i}", data: { sleep_time: i + 1 })
end

def handler(job_name, data)
  sleep data['sleep_time'].to_i
  p "completed job #{job_name}"
end

Rhino::Worker.new(name: 'tester', handler: :handler)

sleep 8

r.add(job_name: 'test_extra', data: { sleep_time: 1 })

loop do
  # keep locally running
end
