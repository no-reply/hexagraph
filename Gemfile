source "https://rubygems.org"

gem 'lmdb'
gem 'rdf'

group :debug do
  gem 'psych', platforms: [:mri, :rbx]
  gem "wirble"
  gem "redcarpet", platforms: :ruby
  gem "byebug", platforms: :mri
  gem 'guard-rspec'
  gem 'benchmark-ips'
end

group :test do
  gem "rake"
  gem "equivalent-xml"
  gem 'fasterer'
end

platforms :rbx do
  gem 'rubysl', '~> 2.0'
  gem 'rubinius', '~> 2.0'
end
