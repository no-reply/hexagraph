require 'bm_helper'

describe 'insert performance', :benchmark => true do
  let(:db) { Hexagraph::Database.new('.tmp/bm', mapsize: 1_000_000_000) }

  def triples(count: 25_000, length: 15)
    Enumerator.new do |yielder|
      loop do
        # make some reasonably dense data; we could probably
        # use a more realistic dataset for these benchmarks
        n1 = (0...length).map { (65 + rand(26)).chr }.join
        n2 = (0...length).map { (65 + rand(26)).chr }.join
        n3 = (0...length).map { (65 + rand(26)).chr }.join
        n4 = (0...length).map { (65 + rand(26)).chr }.join
        p1 = (0...length).map { (65 + rand(26)).chr }.join
        p2 = (0...length).map { (65 + rand(26)).chr }.join
        p3 = (0...length).map { (65 + rand(26)).chr }.join

        yielder.yield [n1, p1, n2]
        yielder.yield [n1, p1, n3]
        yielder.yield [n3, p2, n3]
        yielder.yield [n1, p2, n3]
        yielder.yield [n2, p1, n4]
        yielder.yield [n2, p1, n3]
        yielder.yield [n2, p2, n1]
        yielder.yield [n3, p3, n1]
        yielder.yield [p3, p2, p1]
        yielder.yield [n4, p1, n3]
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
