require 'spec_helper'
require 'rdf/spec/repository'

describe Hexagraph::Repository do
  it_behaves_like 'an RDF::Repository'

  it do
    binding.pry
  end
end
