require 'rdf'

module Hexagraph
  class Repository < RDF::Repository
    def durable?
      true
    end
  end
end
