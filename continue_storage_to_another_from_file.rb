#!/bin/ruby
require 'aws-sdk-v1' 
require 'nokogiri' 
require 'parallel' 
require 'yaml'
require 'stringio'
require 'logger'

require './migrate_storage.rb'


ms = MigrateStorage.new
ms.migrate_from_file ARGV[0]
