# Rails Console Script to Extract Duplicate Lawyers
# Run this in rails console: rails c
# Then copy and paste this code

# Configuration
OUTPUT_DIR = Rails.root.join('tmp', 'duplicate_lawyers')
MIN_DUPLICATES = 2  # Minimum number of lawyers with same name to export

puts "🔍 Starting duplicate lawyer extraction..."
puts "📁 Output directory: #{OUTPUT_DIR}"

# Create output directory if it doesn't exist
FileUtils.mkdir_p(OUTPUT_DIR) unless Dir.exist?(OUTPUT_DIR)

# Find all names that appear more than once
puts "\n📊 Finding duplicate names..."

duplicate_names_query = <<-SQL
  SELECT full_name, COUNT(*) as count 
  FROM lawyers 
  WHERE full_name IS NOT NULL 
    AND full_name != '' 
    AND profile_picture IS NOT NULL 
    AND profile_picture != ''
  GROUP BY full_name 
  HAVING COUNT(*) >= #{MIN_DUPLICATES}
  ORDER BY COUNT(*) DESC, full_name ASC
SQL

duplicate_stats = ActiveRecord::Base.connection.execute(duplicate_names_query)

puts "✅ Found #{duplicate_stats.count} names with duplicates"

# Display top duplicates
puts "\n🔝 Top 10 most duplicated names:"
duplicate_stats.first(10).each_with_index do |row, index|
  name = row['full_name'] || row[0]  # Handle different DB adapters
  count = row['count'] || row[1]
  puts "#{index + 1}. #{name} (#{count} lawyers)"
end

# Process each duplicate name
puts "\n🔄 Processing duplicate lawyers..."

total_exported = 0
total_lawyers = 0

duplicate_stats.each_with_index do |row, index|
  name = row['full_name'] || row[0]
  count = row['count'] || row[1]
  
  puts "\n[#{index + 1}/#{duplicate_stats.count}] Processing: #{name} (#{count} lawyers)"
  
  # Find all lawyers with this name
  lawyers = Lawyer.where(full_name: name)
                  .where.not(profile_picture: [nil, ''])
                  .order(:created_at)
  
  if lawyers.count < MIN_DUPLICATES
    puts "  ⚠️  Skipping - only #{lawyers.count} lawyers found with profile pictures"
    next
  end
  
  # Prepare data for JSON export
  lawyers_data = lawyers.map do |lawyer|
    {
      id: lawyer.id,
      full_name: lawyer.full_name,
      oab_number: lawyer.oab_number,
      oab_id: lawyer.oab_id,
      state: lawyer.state,
      city: lawyer.city,
      profile_picture: lawyer.profile_picture,
      cna_picture: lawyer.cna_picture,
      situation: lawyer.situation,
      profession: lawyer.profession,
      suplementary: lawyer.suplementary,
      address: lawyer.address,
      zip_code: lawyer.zip_code,
      phone_number_1: lawyer.phone_number_1,
      phone_number_2: lawyer.phone_number_2,
      email: lawyer.email,
      created_at: lawyer.created_at,
      updated_at: lawyer.updated_at
    }
  end
  
  # Create safe filename
  safe_filename = name.downcase
                     .gsub(/[àáâãäå]/, 'a')
                     .gsub(/[èéêë]/, 'e')
                     .gsub(/[ìíîï]/, 'i')
                     .gsub(/[òóôõö]/, 'o')
                     .gsub(/[ùúûü]/, 'u')
                     .gsub(/[ç]/, 'c')
                     .gsub(/[ñ]/, 'n')
                     .gsub(/[^a-z0-9\s]/, '')
                     .gsub(/\s+/, '_')
                     .strip
  
  filename = "#{safe_filename}_lawyers.json"
  filepath = OUTPUT_DIR.join(filename)
  
  # Write JSON file
  begin
    File.write(filepath, JSON.pretty_generate(lawyers_data))
    puts "  ✅ Exported #{lawyers.count} lawyers to: #{filename}"
    total_exported += 1
    total_lawyers += lawyers.count
  rescue => e
    puts "  ❌ Error writing file: #{e.message}"
  end
  
  # Add a small delay to avoid overwhelming the system
  sleep(0.1) if index % 10 == 0
end

puts "\n" + "="*60
puts "🎉 EXTRACTION COMPLETE!"
puts "📊 Summary:"
puts "   • Total duplicate name groups: #{duplicate_stats.count}"
puts "   • Files exported: #{total_exported}"
puts "   • Total lawyers exported: #{total_lawyers}"
puts "   • Output directory: #{OUTPUT_DIR}"
puts "="*60

# Optional: Show file sizes
puts "\n📁 Generated files:"
Dir.glob(OUTPUT_DIR.join("*.json")).sort.each do |file|
  size = File.size(file)
  size_kb = (size / 1024.0).round(2)
  basename = File.basename(file)
  puts "   #{basename} (#{size_kb} KB)"
end

# Optional: Generate a summary report
summary_report = {
  extraction_date: Time.current,
  total_duplicate_groups: duplicate_stats.count,
  files_exported: total_exported,
  total_lawyers: total_lawyers,
  output_directory: OUTPUT_DIR.to_s,
  top_duplicates: duplicate_stats.first(20).map do |row|
    {
      name: row['full_name'] || row[0],
      count: row['count'] || row[1]
    }
  end
}

summary_path = OUTPUT_DIR.join('extraction_summary.json')
File.write(summary_path, JSON.pretty_generate(summary_report))
puts "\n📋 Summary report saved to: extraction_summary.json"

puts "\n🚀 Ready for face comparison! Use the Python script on these JSON files."
