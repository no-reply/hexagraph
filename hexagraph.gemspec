#!/usr/bin/env ruby -rubygems
# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.version            = File.read('VERSION').chomp
  gem.date               = File.mtime('VERSION').strftime('%Y-%m-%d')

  gem.name               = 'hexagraph'
  gem.homepage           = 'http://ruby-rdf.org/hexagraph'
  gem.license            = 'Public Domain' if gem.respond_to?(:license=)
  gem.summary            = 'An LMDB-backed graph database and RDF quad store in Ruby.'
  gem.description        = 'An LMDB-backed graph database and RDF quad store in Ruby.'
  gem.authors            = ['Tom Johnson']
  gem.email              = 'public-rdf-ruby@w3.org'

  gem.platform           = Gem::Platform::RUBY
  gem.files              = %w(AUTHORS CREDITS README UNLICENSE VERSION) + Dir.glob('lib/**/*.rb')
  gem.require_paths      = %w(lib)
  gem.extensions         = %w()
  gem.test_files         = %w()
  gem.has_rdoc           = false

  gem.required_ruby_version      = '>= 2.0'
  gem.requirements               = []
  gem.add_runtime_dependency     'lmdb', '~> 0.4'
  gem.add_runtime_dependency     'rdf', '~> 1.99'
  gem.add_development_dependency 'rdf-spec',    '~> 1.1', '>= 1.1.13'
  gem.add_development_dependency 'rspec',       '~> 3.4'
  gem.add_development_dependency 'yard',        '~> 0.8'

  gem.post_install_message       = nil
end
