require "bundler/setup"
require 'hexagraph'

RSpec.configure do |config|
  config.filter_run focus: true
  config.run_all_when_everything_filtered = true
  config.filter_run_excluding benchmark: true
end

Encoding.default_external = Encoding::UTF_8
