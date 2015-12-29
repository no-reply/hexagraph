require 'spec_helper'
require 'rdf/spec/repository'

describe Hexagraph::Repository do
  let(:repository) { described_class.new }

  after do
    FileUtils.rm Dir.glob('.tmp/rdf/*.mdb')
    FileUtils.rmdir Dir.glob('.tmp/rdf')
  end

  it_behaves_like 'an RDF::Repository'
  
  it do
    require 'pry'
    binding.pry
  end
end
