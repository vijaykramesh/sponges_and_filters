#!/usr/bin/env ruby

require './ruby_sponge'

raise "Missing params" unless ARGV[0] && ARGV[1]

SpongesAndFilters::RubySponge.soak(ARGV[0], ARGV[1].to_i)