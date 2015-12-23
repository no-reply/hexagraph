# coding: utf-8
module Hexagraph
  ##
  # Maintains a dictionary between external values and internal ids. Mappings 
  # are stored between the external value (`key`) and the internal `id`; a 
  # reverse index is maintained.
  #
  # IDs are a variable sized integer
  #
  # @note: ids are implemented as a BER-compressed integers, but the specific
  #   guarantee of the interface is a variable size ID. That is, the first ID 
  #   will be (at most) a byte. Larger IDs will grow 
  # 
  # @see Array#pack  for details on Ruby's BER implementation (`#pack('w')`)
  #
  # @todo: clear unused values from the dictionary; reuse keys
  #
  # @todo: explore options for caching; a simplified benchmarks for 
  #   `LMDB::Database#get` vs `Hash#[]`, for small datasets:
  #          get     200.375k (±11.2%) i/s -    986.190k
  #          hash      4.696M (±10.1%) i/s -     23.071M
  class Dictionary
    # A separator character guaranteed not to appear in the IDs
    SEPARATOR = "\xFF".force_encoding('ASCII-8BIT')

    ##
    # @param env [LMDB::Environment]
    def initialize(env, create: true)
      @db  = env.database('dict', create: create)
      @get_cache = {}
      @inv = env.database('idict', create: create)
      @lookup_cache = {}
    end

    ##
    # Clears the dictionary
    def clear!
      @db.env.transaction do
        @db.clear
        @inv.clear
      end
    end

    ##
    # Retrieves the database id for a given external value. If the key is not
    # already present, this assigns it a new ID and adds it to the indexes.
    #
    # @param key [String]
    # @return [String] an id
    def get(key)
      response = @db.get(key)
      return response if response
      
      add(key)
    end

    ##
    # Performs reverse lookup on an `id` to retrieve the associated key.
    #
    # @param id [String] an id
    # @return [String, nil] a database value key; nil if none is present
    def lookup(id)
      @inv.get(id)
    end

    private

    ##
    # @private
    # Adds a value to the dictionary, assigning an id to be used in the graph.
    #
    # @todo: id selection is naive, and is likely subject to horrible race 
    #   conditions.
    def add(key)
      id = begin
             @inv.cursor { |c| c.last.first }
               .unpack('w').map { |i| i + 1 }.pack('w')
           rescue LMDB::Error::NOTFOUND
             "\x00"
           end
      
      @db.env.transaction do
        @db.put(key, id)
        @get_cache[key] = id
        @inv.put(id, key)
        @lookup_cache[id] = key
      end

      id
    end
  end
end
