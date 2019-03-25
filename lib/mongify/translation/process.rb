require 'mongify/translation/processor_common'
module Mongify
  class Translation
    #
    # This module does the processing on the translation object
    #
    include ProcessorCommon
    module Process
      # Does the actual act of processing the translation.
      # Takes in both a sql connection and a no sql connection
      def process(sql_connection, no_sql_connection, options)
        prepare_connections(sql_connection, no_sql_connection, options)
        setup_db_index
        copy_data
        update_reference_ids
        copy_embedded_tables
        copy_polymorphic_tables
        remove_pre_mongified_ids
        nil
      end

      #######
      private
      #######

      # Does the straight copy (of tables)
      def copy_data
        Parallel.each(self.copy_tables, in_processes: self.processes, progress:"Copying") do |t|
          sql_connection.select_rows(t.sql_name) do |rows, page, total_pages|
            insert_rows = []
            rows.each do |row|
              insert_rows << t.translate(row)
            end
            no_sql_connection.insert_into(t.name, insert_rows) unless insert_rows.empty?
          end
        end
      end

      # Updates the reference ids in the no sql database
      def update_reference_ids
        Parallel.each(self.copy_tables, in_processes: self.processes, progress:'Updating References') do |t|
          rows = no_sql_connection.select_rows(t.name)
          rows.each do |row|
            id = row["_id"]
            attributes = fetch_reference_ids(t, row)
            no_sql_connection.connection[t.name].update_one( { "_id" => id } , { "$set"  => attributes}) unless attributes.blank?
          end
        end
      end

    end
  end
end
