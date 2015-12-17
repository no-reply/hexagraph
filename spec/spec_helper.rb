require "bundler/setup"
require 'hexagraph'

RSpec.configure do |config|
  config.filter_run focus: true
  config.run_all_when_everything_filtered = true
  config.exclusion_filter = {ruby: lambda { |version|
    RUBY_VERSION.to_s !~ /^#{version}/
  }}
end

Encoding.default_external = Encoding::UTF_8
