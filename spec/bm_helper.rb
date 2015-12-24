require "bundler/setup"
require 'hexagraph'

require 'benchmark'
require 'benchmark/ips'
require 'fileutils'

RSpec.configure do |config|
  config.before(:suite) do
    FileUtils::mkdir_p '.tmp/bm'
  end

  config.after(:suite) do
    FileUtils.rm Dir.glob('.tmp/bm/*.mdb')
    FileUtils.rmdir Dir.glob('.tmp/bm')
  end
end
