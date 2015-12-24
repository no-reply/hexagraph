require 'bm_helper'
require 'pry'

describe 'insert performance', :benchmark => true do
  let(:db) { Hexagraph::Database.new('.tmp/bm', mapsize: 1_000_000_000) }

  def triples(count: 25_000, length: 15)
    Enumerator.new do |yielder|
      loop do
        s = (0...length).map { (65 + rand(26)).chr }.join
        p = (0...length).map { (65 + rand(26)).chr }.join
        o = (0...length).map { (65 + rand(26)).chr }.join

        yielder.yield [s, p, o]
        yielder.yield [s, o, p]
        # yielder.yield [p, s, o]
        # yielder.yield [p, o, s]
        # yielder.yield [o, p, s]
        yielder.yield [o, s, p]
      end
    end.take(count)
  end

  before { db.clear! }

  it do
    Benchmark.bm(14) do |bm|
      [10, 100, 1_000, 10_000, 25_000, 50_000].each do |count|
        triple_enums = Enumerator.new { |y| loop { y.yield triples(count: count) }}.take(100).to_enum
    
        bm.report("insert #{count}\tx 100") do
          triple_enums.each { |ts| db.inserts(ts) }
        end
      end
    end
  end
end
