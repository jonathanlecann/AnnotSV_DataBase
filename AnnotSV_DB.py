import sqlite3
import csv
import argparse
import os

def create_database(db_path):
    """
    Creates the SQLite database with tables for samples, genes, SV, Tx (transcripts)
    with many-to-many relationships.
    """
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable foreign key constraints for referential integrity
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Create table to store sample information
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS samples (
            sample_id TEXT PRIMARY KEY
        );
    """)

    # Create table to store gene information
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genes (
            gene_id INTEGER PRIMARY KEY AUTOINCREMENT,
            gene_name TEXT NOT NULL UNIQUE
        );
    """)

    # Create table to store structural variant information
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS SV (
            SV_id INTEGER PRIMARY KEY AUTOINCREMENT,
            AnnotSV_ID TEXT NOT NULL,
            SV_chrom TEXT NOT NULL,
            SV_start INTEGER NOT NULL,
            SV_end INTEGER NOT NULL,
            SV_type TEXT NOT NULL,
            Annotation_mode TEXT NOT NULL
        );
    """)

    # Create table to store Transcripts informations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Tx (
            Tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
            Tx_name TEXT NOT NULL,
            Tx_version TEXT,
            Tx_start INTEGER NOT NULL,
            Tx_end INTEGER NOT NULL,
            UNIQUE(Tx_name, Tx_version)  -- Ensure each transcript+version combination is unique
        );
    """)

    # Create junction table for many-to-many relationship between SV and samples
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sv_samples (
            SV_id INTEGER NOT NULL,
            sample_id TEXT NOT NULL,
            PRIMARY KEY (SV_id, sample_id),
            FOREIGN KEY (SV_id) REFERENCES SV(SV_id) ON DELETE CASCADE,
            FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
        );
    """)

    # Create junction table for many-to-many relationship between SV and genes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sv_genes (
            SV_id INTEGER NOT NULL,
            gene_id INTEGER NOT NULL,
            PRIMARY KEY (SV_id, gene_id),
            FOREIGN KEY (SV_id) REFERENCES SV(SV_id) ON DELETE CASCADE,
            FOREIGN KEY (gene_id) REFERENCES genes(gene_id) ON DELETE CASCADE
        );
    """)

    #Create new table : sv_tx (Association between SV and Transcripts)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sv_tx (
            SV_id INTEGER NOT NULL,
            Tx_id INTEGER NOT NULL,
            Overlapped_tx_length INTEGER,         
            Overlapped_CDS_length INTEGER,        
            Overlapped_CDS_percent REAL,          
            Frameshift TEXT,                      
            Exon_count INTEGER,                   
            Location TEXT,                        
            Location2 TEXT,                       
            Dist_nearest_SS INTEGER,              
            Nearest_SS_type TEXT,                 
            Intersect_start INTEGER,              
            Intersect_end INTEGER,                
            PRIMARY KEY (SV_id, Tx_id),
            FOREIGN KEY (SV_id) REFERENCES SV(SV_id) ON DELETE CASCADE,
            FOREIGN KEY (Tx_id) REFERENCES Tx(Tx_id) ON DELETE CASCADE
        );
    """)

    # Create indexes for better query performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sv_annotsv ON SV(AnnotSV_ID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sv_position ON SV(SV_chrom, SV_start, SV_end);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_genes_name ON genes(gene_name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_name ON Tx(Tx_name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_position ON Tx(Tx_start, Tx_end);")

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print(f"✅ Database {db_path} created successfully.")

def check_annotation_mode(value):
    """
    Validates and normalizes the annotation mode value.
    Must be either 'full' or 'split'.
    Exits the program if invalid.
    """
    if not value:
        exit("❌ Error: Invalid annotation mode. Must be 'full' or 'split'.")
    
    value = value.strip().lower()
    
    if value not in ['full', 'split']:
        exit(f"❌ Error: Invalid annotation mode '{value}'. Must be 'full' or 'split'.")
    
    return value

def normalize_sample_id(sample_id):
    """
    Normalizes sample ID by handling empty values.
    Returns 'NA' for empty or None values.
    """
    if not sample_id:
        return "NA"
    
    sample_id = sample_id.strip()
    return sample_id

def parse_multiple_samples(samples_string):
    """
    Parses a comma-separated string of sample IDs into a list.
    Returns a list with at least one element ('NA' if empty).
    """
    samples = [normalize_sample_id(s.strip()) for s in samples_string.split(',') if normalize_sample_id(s.strip())]
    return samples or ["NA"]

def get_or_create_gene(cursor, gene_name):
 
    # Step 1: Try to find if this gene already exists in the database
    cursor.execute("SELECT gene_id FROM genes WHERE gene_name = ?", (gene_name,))
    result = cursor.fetchone()
    
    # Step 2: Check the result
    if result:
        # Gene found! Return its existing ID (no duplicate created)
        return result[0]
    else:
        # Gene not found! Create a new entry for this gene
        cursor.execute("INSERT INTO genes (gene_name) VALUES (?)", (gene_name,))
        # Return the ID of the gene we just created
        return cursor.lastrowid

def get_or_create_transcript(cursor, tx_name, tx_version, tx_start, tx_end):
    """
    ================ TRANSCRIPT HANDLING FUNCTION ================
    This function manages transcript entries in the Tx table.
    - If the transcript already exists: returns its existing Tx_id
    - If the transcript doesn't exist: creates it and returns the new Tx_id
    
    Parameters:
    - tx_name: The transcript identifier (e.g., "NM_001111125")
    - tx_version: The transcript version number
    - tx_start: The genomic start coordinate of the transcript
    - tx_end: The genomic end coordinate of the transcript
    
    Returns:
    - Tx_id: The database ID of the transcript
    """
    # First, check if this transcript+version combination already exists
    cursor.execute("""
        SELECT Tx_id FROM Tx 
        WHERE Tx_name = ? AND (Tx_version = ? OR (Tx_version IS NULL AND ? IS NULL))
    """, (tx_name, tx_version, tx_version))
    result = cursor.fetchone()
    
    if result:
        # Transcript found! Return its existing ID
        return result[0]
    else:
        # Transcript not found! Create a new entry
        cursor.execute("""
            INSERT INTO Tx (Tx_name, Tx_version, Tx_start, Tx_end) 
            VALUES (?, ?, ?, ?)
        """, (tx_name, tx_version, tx_start, tx_end))
        # Return the ID of the transcript we just created
        return cursor.lastrowid

def parse_numeric_value(value, default=None):
    """
    Helper function to parse numeric values from the TSV file.
    Returns the parsed value or the default if parsing fails.
    """
    if not value or value.strip() == '':
        return default
    try:
        # Check if it's a float
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except (ValueError, TypeError):
        return default

def import_tsv(db_path, tsv_file_path):
    """
    Imports data from a TSV file into the database.
    Only imports 'full' annotation mode entries as unique SVs.
    """
    # Validate input file exists
    if not os.path.exists(tsv_file_path):
        print(f"❌ Error: File not found: {tsv_file_path}")
        return

    # Validate database exists
    if not os.path.exists(db_path):
        print(f"❌ Error: Database not found. Please create it first with --create")
        return

    # Connect to database with foreign keys enabled
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # PHASE 1: Collect all unique samples from the file
    print(f"✅ Phase 1: Collecting all samples from file...")
    all_samples = set()
    
    with open(tsv_file_path, 'r', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        sample_column = None
        for col in reader.fieldnames:
            if 'Samples_ID' == col:
                sample_column = col
                break

        if sample_column:
            print(f"   Found samples column: {sample_column}")
            for row in reader:
                samples_string = row.get(sample_column, '').strip()
                sample_names = parse_multiple_samples(samples_string)
                all_samples.update(sample_names)

    # Insert all samples into database
    display_samples = {s for s in all_samples if s not in ('NA')}
    print(f"✅ Inserting {len(all_samples)} total samples (including NA)...")
    for sample in all_samples:
        cursor.execute("INSERT OR IGNORE INTO samples (sample_id) VALUES (?)", (sample,))

    conn.commit()
    print(f"   ✅ Samples inserted and committed")
    print(f"   ✅ Verified: {len(display_samples)} samples in database: {', '.join(sorted(display_samples))}")

    # PHASE 2: GENE COLLECTION
    # We collect gene names only from 'split' lines because each split line 
    # represents one gene that is actually overlapped by a structural variant
    print(f"\n✅ Phase 2: Collecting genes from split lines...")
    all_genes = set()  # Use a set to automatically avoid duplicate gene names
    
    with open(tsv_file_path, 'r', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        # Find the columns we need in the TSV file
        gene_column = None
        annotation_mode_column = None
        for col in reader.fieldnames:
            if 'Gene_name' == col:
                gene_column = col
            elif 'Annotation_mode' == col:
                annotation_mode_column = col

        # Process each line to extract gene names from split lines only
        if gene_column and annotation_mode_column:
            print(f"   Found genes column: {gene_column}")
            for row in reader:
                # Get the annotation mode (full or split) and normalize it
                annotation_mode = row.get(annotation_mode_column, '').strip().lower()
                
                # Only process split lines - these contain individual overlapped genes
                if annotation_mode == 'split':  # Only split lines
                    # Extract the gene name from this split line
                    gene_name = row.get(gene_column, '').strip()
                    if gene_name:
                        # Add to our collection of unique genes
                        all_genes.add(gene_name)

    # Insert all unique genes into the genes table
    print(f"✅ Inserting {len(all_genes)} unique genes...")
    for gene in all_genes:
        # INSERT OR IGNORE means: add this gene only if it doesn't already exist
        cursor.execute("INSERT OR IGNORE INTO genes (gene_name) VALUES (?)", (gene,))

    conn.commit()  # Save all gene insertions to the database
    print(f"   ✅ Genes inserted and committed")
    print(f"   ✅ Gene overview: {', '.join(sorted(list(all_genes))[:10])}")

    # PHASE 3: Process structural variants and transcripts
    print(f"\n✅ Phase 3: Processing structural variants and transcripts...")
    
    total_rows = 0
    imported_rows = 0
    skipped_rows = 0
    full_count = 0
    split_count = 0
    processed_sv = set()
    transcript_count = 0

    with open(tsv_file_path, 'r', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')

        # Initialize column variables
        sample_column = None
        gene_column = None
        Annotation_mode_column = None
        AnnotSV_ID_column = None
        SV_chrom_column = None
        SV_start_column = None
        SV_end_column = None
        SV_type_column = None
        
        # ================ NEW COLUMNS FOR TRANSCRIPTS ================
        Tx_column = None
        Tx_version_column = None
        Tx_start_column = None
        Tx_end_column = None
        
        # ================ COLUMNS FOR SV-TX ASSOCIATION ================
        Overlapped_tx_length_column = None
        Overlapped_CDS_length_column = None
        Overlapped_CDS_percent_column = None
        Frameshift_column = None
        Exon_count_column = None
        Location_column = None
        Location2_column = None
        Dist_nearest_SS_column = None
        Nearest_SS_type_column = None
        Intersect_start_column = None
        Intersect_end_column = None
        
        # Detect column names by exact match
        for val in reader.fieldnames:
            if val == 'Samples_ID':
                sample_column = val
            elif val == 'Gene_name':
                gene_column = val
            elif val == 'Annotation_mode':
                Annotation_mode_column = val
            elif val == 'AnnotSV_ID':
                AnnotSV_ID_column = val
            elif val == 'SV_chrom':
                SV_chrom_column = val
            elif val == 'SV_start':
                SV_start_column = val
            elif val == 'SV_end':
                SV_end_column = val
            elif val == 'SV_type':
                SV_type_column = val
            # ================ DETECT TRANSCRIPT COLUMNS ================
            elif val == 'Tx':
                Tx_column = val
            elif val == 'Tx_version':
                Tx_version_column = val
            elif val == 'Tx_start':
                Tx_start_column = val
            elif val == 'Tx_end':
                Tx_end_column = val
            # ================ DETECT ASSOCIATION COLUMNS ================
            elif val == 'Overlapped_tx_length':
                Overlapped_tx_length_column = val
            elif val == 'Overlapped_CDS_length':
                Overlapped_CDS_length_column = val
            elif val == 'Overlapped_CDS_percent':
                Overlapped_CDS_percent_column = val
            elif val == 'Frameshift':
                Frameshift_column = val
            elif val == 'Exon_count':
                Exon_count_column = val
            elif val == 'Location':
                Location_column = val
            elif val == 'Location2':
                Location2_column = val
            elif val == 'Dist_nearest_SS':
                Dist_nearest_SS_column = val
            elif val == 'Nearest_SS_type':
                Nearest_SS_type_column = val
            elif val == 'Intersect_start':
                Intersect_start_column = val
            elif val == 'Intersect_end':
                Intersect_end_column = val

        # Process each row in the TSV file
        for row in reader:
            total_rows += 1

            # Extract and validate annotation mode
            Annotation_mode = row.get(Annotation_mode_column, '').strip().lower() if Annotation_mode_column else ''
            Annotation_mode = check_annotation_mode(Annotation_mode)

            # Only process full lines for SVs
            if Annotation_mode == 'full':
                full_count += 1
            elif Annotation_mode == 'split':
                split_count += 1
                continue  # Skip split lines for SV creation  

            # Extract sample IDs from full lines
            samples_string = row.get(sample_column, '').strip() if sample_column else ''
            sample_names = parse_multiple_samples(samples_string)

            # Extract SV information with error handling
            try:
                AnnotSV_ID = row.get(AnnotSV_ID_column, '').strip() if AnnotSV_ID_column else ''
                SV_chrom = row.get(SV_chrom_column, '').strip() if SV_chrom_column else ''
                
                SV_start = int(row.get(SV_start_column, '').strip()) if SV_start_column and row.get(SV_start_column, '').strip() else 0
                SV_end = int(row.get(SV_end_column, '').strip()) if SV_end_column and row.get(SV_end_column, '').strip() else 0
                SV_type = row.get(SV_type_column, '').strip() if SV_type_column else ''
            except (ValueError, TypeError):
                skipped_rows += 1
                continue

            # Create unique key for SV to check duplicates
            sv_key = (AnnotSV_ID, SV_chrom, SV_start, SV_end, Annotation_mode)
            
            # Skip if this SV was already processed
            if sv_key in processed_sv:
                continue

            # Insert new SV into database
            cursor.execute("""
                INSERT OR IGNORE INTO SV (
                    AnnotSV_ID, SV_chrom, SV_start, SV_end, SV_type, Annotation_mode
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (AnnotSV_ID, SV_chrom, SV_start, SV_end, SV_type, Annotation_mode))
            
            processed_sv.add(sv_key)
            imported_rows += 1

            # Get the SV_id for creating relationships
            cursor.execute("SELECT SV_id FROM SV WHERE AnnotSV_ID = ? AND Annotation_mode = ?", (AnnotSV_ID, Annotation_mode))
            result = cursor.fetchone()
            
            if result:
                sv_id = result[0]
                
                # Create SV-sample relationships 
                for sample in sample_names:
                    cursor.execute(
                        "INSERT OR IGNORE INTO sv_samples (SV_id, sample_id) VALUES (?, ?)",
                        (sv_id, sample)
                    )

    # PHASE 4: CREATE GENE-SV RELATIONSHIPS AND TRANSCRIPT ASSOCIATIONS
    print(f"\n✅ Phase 4: Creating SV-gene relationships and transcript associations from split lines...")
    
    with open(tsv_file_path, 'r', encoding='latin-1', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        gene_relationships = 0  # Counter for created gene relationships
        tx_relationships = 0    # Counter for created transcript relationships
        
        # Process each line in the file again
        for row in reader:
            # Get the annotation mode for this line
            annotation_mode = row.get(Annotation_mode_column, '').strip().lower()
            
            # Only process split lines - these contain the actual gene-SV overlaps and transcript info
            if annotation_mode == 'split':
                # Extract the AnnotSV_ID and gene name from this split line
                annotsv_id = row.get(AnnotSV_ID_column, '').strip()
                gene_name = row.get(gene_column, '').strip()
                
                # ================ PROCESS TRANSCRIPT INFORMATION ================
                # Extract transcript data from the split line
                tx_name = row.get(Tx_column, '').strip() if Tx_column else ''
                tx_version = row.get(Tx_version_column, '').strip() if Tx_version_column else None
                tx_start_str = row.get(Tx_start_column, '').strip() if Tx_start_column else ''
                tx_end_str = row.get(Tx_end_column, '').strip() if Tx_end_column else ''
                
                # Process transcript if we have the required information
                if annotsv_id and tx_name and tx_start_str and tx_end_str:
                    try:
                        tx_start = int(tx_start_str)
                        tx_end = int(tx_end_str)
                        
                        # Get or create the transcript in the Tx table
                        tx_id = get_or_create_transcript(cursor, tx_name, tx_version, tx_start, tx_end)
                        transcript_count += 1
                        
                        # Find the SV record that corresponds to this AnnotSV_ID
                        cursor.execute("SELECT SV_id FROM SV WHERE AnnotSV_ID = ? AND Annotation_mode = 'full'", (annotsv_id,))
                        sv_result = cursor.fetchone()
                        
                        if sv_result:
                            sv_id = sv_result[0]
                            
                            # ================ CREATE SV-TRANSCRIPT ASSOCIATION ================
                            # Extract all the association details from the split line
                            overlapped_tx_length = parse_numeric_value(row.get(Overlapped_tx_length_column, ''))
                            overlapped_cds_length = parse_numeric_value(row.get(Overlapped_CDS_length_column, ''))
                            overlapped_cds_percent = parse_numeric_value(row.get(Overlapped_CDS_percent_column, ''))
                            frameshift = row.get(Frameshift_column, '').strip() if Frameshift_column else None
                            exon_count = parse_numeric_value(row.get(Exon_count_column, ''))
                            location = row.get(Location_column, '').strip() if Location_column else None
                            location2 = row.get(Location2_column, '').strip() if Location2_column else None
                            dist_nearest_ss = parse_numeric_value(row.get(Dist_nearest_SS_column, ''))
                            nearest_ss_type = row.get(Nearest_SS_type_column, '').strip() if Nearest_SS_type_column else None
                            intersect_start = parse_numeric_value(row.get(Intersect_start_column, ''))
                            intersect_end = parse_numeric_value(row.get(Intersect_end_column, ''))
                            
                            # Insert the SV-transcript relationship with all details
                            cursor.execute("""
                                INSERT OR IGNORE INTO sv_tx (
                                    SV_id, Tx_id, Overlapped_tx_length, Overlapped_CDS_length,
                                    Overlapped_CDS_percent, Frameshift, Exon_count, Location,
                                    Location2, Dist_nearest_SS, Nearest_SS_type, Intersect_start,
                                    Intersect_end
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (sv_id, tx_id, overlapped_tx_length, overlapped_cds_length,
                                  overlapped_cds_percent, frameshift, exon_count, location,
                                  location2, dist_nearest_ss, nearest_ss_type, intersect_start,
                                  intersect_end))
                            tx_relationships += 1
                            
                    except (ValueError, TypeError) as e:
                        # Skip if we can't parse the transcript coordinates
                        pass
                
                # ================ PROCESS GENE RELATIONSHIPS ================
                # Create gene-SV relationship if we have both pieces of information
                if annotsv_id and gene_name:
                    # Find the SV record that corresponds to this AnnotSV_ID
                    cursor.execute("SELECT SV_id FROM SV WHERE AnnotSV_ID = ? AND Annotation_mode = 'full'", (annotsv_id,))
                    sv_result = cursor.fetchone()
                    
                    if sv_result:
                        sv_id = sv_result[0]  # Get the SV ID
                        
                        # Get the gene ID (create the gene if it doesn't exist)
                        gene_id = get_or_create_gene(cursor, gene_name)
                        
                        # Create the relationship between this SV and this gene
                        cursor.execute(
                            "INSERT OR IGNORE INTO sv_genes (SV_id, gene_id) VALUES (?, ?)",
                            (sv_id, gene_id)
                        )
                        gene_relationships += 1

    conn.commit()  # Save all relationships to the database
    print(f"   ✅ Created {gene_relationships} SV-gene relationships")
    print(f"   ✅ Created {tx_relationships} SV-transcript relationships")
    print(f"   ✅ Processed {transcript_count} unique transcripts")

    # Generate statistics
    cursor.execute("SELECT COUNT(*) FROM sv_samples")
    total_sample_links = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM sv_genes")
    total_gene_links = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM sv_tx")
    total_tx_links = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM Tx")
    total_transcripts = cursor.fetchone()[0]

    # Differential analysis for SV-sample relationships
    cursor.execute("""
        SELECT sample_count, COUNT(*) as sv_count
        FROM (
            SELECT sv.SV_id, COUNT(ss.sample_id) as sample_count
            FROM SV sv
            JOIN sv_samples ss ON sv.SV_id = ss.SV_id
            WHERE sv.Annotation_mode = 'full'
            GROUP BY sv.SV_id
        )
        GROUP BY sample_count
        ORDER BY sample_count
    """)
    
    distribution = cursor.fetchall()
    distribution_text = []
    calculation_parts = []
    for count, nb_sv in distribution:
        distribution_text.append(f"{nb_sv} SV present in {count} sample(s)")
        calculation_parts.append(f"{nb_sv}x{count}")

    # Print import summary
    print(f"\n✅ Import completed!")
    print(f"\n✅ Summary:")
    print(f"   File statistics:")
    print(f"   - {total_rows} total variant lines processed")
    print(f"   - {skipped_rows} variant lines skipped (invalid data)")
    print(f"   - {full_count} full lines")
    print(f"   - {split_count} split lines")
    print(f"\n   Database statistics:")
    print(f"   - {imported_rows} new SV records imported (full annotations only)")
    print(f"   - {total_transcripts} unique transcripts imported")
    print(f"   - {total_sample_links} total SV-sample relationships created")
    print(f"   - {total_gene_links} total SV-gene relationships created")
    print(f"   - {total_tx_links} total SV-transcript relationships created")
    print(f"   - Differential analysis ({imported_rows} SV → {total_sample_links} relations):")
    for dist in distribution_text:
        print(f"     {dist}")
    print(f"     = {total_sample_links} relations ({' + '.join(calculation_parts)})")

    # Generate sample statistics
    print(f"\n   Top samples by SV count:")
    cursor.execute("""
        SELECT s.sample_id, COUNT(DISTINCT ss.SV_id) as sv_count
        FROM samples s
        LEFT JOIN sv_samples ss ON s.sample_id = ss.sample_id
        GROUP BY s.sample_id
        ORDER BY sv_count DESC
    """)
    
    rows = cursor.fetchall()
    for sample_id, count in rows:
        if sample_id in ('NA', ''):
            print(f"   - NA and Empty : {count} SVs")
        else:
            print(f"   - {sample_id}: {count} SVs")

    # Generate gene statistics - show which genes are most frequently affected
    print(f"\n   Genes most frequently affected by structural variants:")
    cursor.execute("""
        SELECT g.gene_name, COUNT(DISTINCT sg.SV_id) as sv_count
        FROM genes g
        LEFT JOIN sv_genes sg ON g.gene_id = sg.gene_id
        GROUP BY g.gene_id, g.gene_name
        HAVING sv_count > 0
        ORDER BY sv_count DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    for gene_name, count in rows:
        print(f"   - {gene_name}: {count} SVs")

    # Generate Transcripts statistics - show which transcripts are most affected by SVs
    print(f"\n   Transcripts most frequently affected by structural variants:")
    cursor.execute("""
        SELECT t.Tx_name, t.Tx_version, COUNT(DISTINCT st.SV_id) as sv_count,
               AVG(st.Overlapped_CDS_percent) as avg_cds_overlap
        FROM Tx t
        LEFT JOIN sv_tx st ON t.Tx_id = st.Tx_id
        WHERE st.SV_id IS NOT NULL
        GROUP BY t.Tx_id, t.Tx_name, t.Tx_version
        ORDER BY sv_count DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    for tx_name, tx_version, count, avg_overlap in rows:
        version_str = f" v{tx_version}" if tx_version else ""
        overlap_str = f" (avg CDS overlap: {avg_overlap:.1f}%)" if avg_overlap else ""
        print(f"   - {tx_name}{version_str}: {count} SVs{overlap_str}")

    # Generate which SVs is causing frameshifts
    print(f"\n   Structural variants causing frameshifts:")
    cursor.execute("""
        SELECT sv.AnnotSV_ID, COUNT(DISTINCT st.Tx_id) as affected_transcripts
        FROM SV sv
        JOIN sv_tx st ON sv.SV_id = st.SV_id
        WHERE st.Frameshift = 'yes'
        GROUP BY sv.SV_id, sv.AnnotSV_ID
        ORDER BY affected_transcripts DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    if rows:
        for annotsv_id, count in rows:
            print(f"   - {annotsv_id}: causes frameshift in {count} transcript(s)")
    else:
        print(f"   - No frameshifts detected")

    # Close database connection
    conn.close()

def main():
    """
    Main function that handles command-line arguments and executes appropriate actions.
    """
    parser = argparse.ArgumentParser(description='Import structural variants with samples, genes, and transcripts relationships')
    parser.add_argument('--create', action='store_true', help='Create the database')
    parser.add_argument('--import', dest='tsv_file', help='Import TSV file')
    parser.add_argument('--db', default='sv_samples.db', help='Database path (default: sv_samples.db)')
    
    args = parser.parse_args()

    if args.create:
        create_database(args.db)
    elif args.tsv_file:
        import_tsv(args.db, args.tsv_file)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
