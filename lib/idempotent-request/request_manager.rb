module IdempotentRequest
  class RequestManager
    attr_reader :request, :storage

    def initialize(request, config)
      @request = request
      @storage = config.fetch(:storage)
      @callback = config[:callback]
    end

    def lock
      storage.lock(key)
    end

    def unlock
      storage.unlock(key)
    end

    def read
      status, headers, response, cached_post_data = parse_data(storage.read(key)).values
      return unless status
      post_data = request.body.read
      request.body.rewind
      return bad_request_response unless cached_post_data == post_data
      run_callback(:detected, key: request.key)
      [status, headers, response]
    end

    def write(*data)
      status, headers, response = data
      post_data = request.body.read
      request.body.rewind
      response = response.body if response.respond_to?(:body)
      if (200..226).cover?(status)
        storage.write(key, payload(status, headers, response, post_data))
      else
        unlock
      end
      data
    end

    private

    def bad_request_response
      [
        400,
        {"Content-Type"=>"application/json; charset=utf-8"},
        [{error: 'Request inconsistent with the supplied idempotent key'}.to_json]
      ]
    end

    def parse_data(data)
      return {} if data.to_s.empty?
      Oj.load(data)
    end

    def payload(status, headers, response, post_data)
      Oj.dump(status: status,
              headers: headers.to_h,
              response: Array(response),
              post_data: post_data)
    end

    def run_callback(action, args)
      return unless @callback

      @callback.new(request).send(action, args)
    end

    def key
      request.key
    end
  end
end
