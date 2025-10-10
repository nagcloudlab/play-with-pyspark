# ========================================
# GraphX Example 1: Fixed Setup & Simple Graph
# ========================================

# This version handles the GraphFrames setup properly and provides alternatives

print("🕸️ Creating Your First Graph with Spark GraphX!")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Method 1: Try to setup Spark with GraphFrames JAR
print("🔧 Setting up Spark with GraphFrames...")

try:
    # This tries to auto-download GraphFrames (may not always work)
    spark = SparkSession.builder \
        .appName("GraphXExample1") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .getOrCreate()
    
    print("✅ Spark session created with GraphFrames package!")
    
    # Try to import GraphFrames
    from graphframes import GraphFrame
    graphframes_available = True
    print("✅ GraphFrames imported successfully!")
    
except Exception as e:
    print("⚠️ GraphFrames package setup failed, trying basic Spark...")
    
    # Fallback: Basic Spark without GraphFrames
    spark = SparkSession.builder \
        .appName("GraphXExample1") \
        .master("local[*]") \
        .getOrCreate()
    
    graphframes_available = False
    print("✅ Basic Spark session created!")
    print("ℹ️ We'll show you the concepts without GraphFrames for now")

print("\n🧑‍🤝‍🧑 Let's create a simple social network!")
print("   We'll model friendships between 6 people")

# Step 1: Create VERTICES (the people in our network)
print("\n👥 Step 1: Creating Vertices (People)")

vertices_data = [
    ("1", "Alice", 25),
    ("2", "Bob", 30), 
    ("3", "Charlie", 35),
    ("4", "Diana", 28),
    ("5", "Eve", 32),
    ("6", "Frank", 27)
]

vertices_df = spark.createDataFrame(vertices_data, ["id", "name", "age"])

print("📊 Our people (vertices):")
vertices_df.show()

# Step 2: Create EDGES (the friendships between people) 
print("\n🔗 Step 2: Creating Edges (Friendships)")

edges_data = [
    ("1", "2", "friend"),    # Alice <-> Bob
    ("2", "3", "friend"),    # Bob <-> Charlie  
    ("3", "4", "friend"),    # Charlie <-> Diana
    ("4", "5", "friend"),    # Diana <-> Eve
    ("1", "3", "friend"),    # Alice <-> Charlie
    ("2", "5", "friend"),    # Bob <-> Eve
    ("4", "6", "friend"),    # Diana <-> Frank
]

edges_df = spark.createDataFrame(edges_data, ["src", "dst", "relationship"])

print("📊 Our friendships (edges):")
edges_df.show()

# Step 3: Graph Analysis (with or without GraphFrames)
print("\n🕸️ Step 3: Graph Analysis")

if graphframes_available:
    try:
        # Create the GraphFrame
        graph = GraphFrame(vertices_df, edges_df)
        
        print("✅ Graph created successfully!")
        print(f"   📍 Vertices (people): {graph.vertices.count()}")
        print(f"   🔗 Edges (friendships): {graph.edges.count()}")
        
        # Advanced graph operations
        print("\n🔍 Advanced Graph Analysis:")
        
        # Find friends of Alice
        alice_friends = graph.edges.filter(graph.edges.src == "1") \
            .join(vertices_df, graph.edges.dst == vertices_df.id) \
            .select("name")
        
        print("\n👥 Alice's friends:")
        alice_friends.show()
        
        # Find mutual friends (triangle counting concept)
        print("\n🔺 Finding potential mutual friend connections...")
        mutual_connections = graph.edges.alias("e1") \
            .join(graph.edges.alias("e2"), 
                  (graph.edges.dst == edges_df.src) & 
                  (graph.edges.src != edges_df.dst)) \
            .select("e1.src", "e2.dst") \
            .distinct()
        
        print("Potential new friendships (people with mutual friends):")
        mutual_connections.show()
        
    except Exception as e:
        print(f"❌ GraphFrames error: {e}")
        print("   Falling back to manual analysis...")
        graphframes_available = False

if not graphframes_available:
    print("📊 Manual Graph Analysis (without GraphFrames):")
    
    # Basic graph statistics using pure Spark SQL
    total_people = vertices_df.count()
    total_friendships = edges_df.count()
    
    print(f"   👥 Total people: {total_people}")
    print(f"   🤝 Total friendships: {total_friendships}")
    print(f"   📈 Average friends per person: {(total_friendships * 2) / total_people:.1f}")
    
    # Find who has the most friends
    print("\n🏆 Most Connected People:")
    friend_counts = edges_df.groupBy("src").count() \
        .join(vertices_df, edges_df.src == vertices_df.id) \
        .select("name", "count") \
        .orderBy("count", ascending=False)
    
    friend_counts.show()
    
    # Show readable friendships
    print("\n🤝 All friendships (readable format):")
    friendships_readable = edges_df \
        .join(vertices_df.alias("person1"), edges_df.src == vertices_df.id) \
        .select("dst", "person1.name") \
        .join(vertices_df.alias("person2"), edges_df.dst == vertices_df.id) \
        .select("person1.name", "person2.name")
    
    friendships_readable.show()
    
    # Find Alice's friends manually
    print("\n👥 Alice's friends:")
    alice_friends = edges_df.filter(edges_df.src == "1") \
        .join(vertices_df, edges_df.dst == vertices_df.id) \
        .select("name")
    alice_friends.show()

print("\n🎯 Network Visualization (Text-based):")
visualization = """
    Alice(1) ---- Bob(2) ---- Charlie(3) ---- Diana(4) ---- Eve(5)
      |            |                           |
      |            |                           |
      +----------- Charlie(3)                 Frank(6)
                   |
                   +------------------------- Eve(5)
"""
print(visualization)

print("\n🧠 Network Analysis:")
analysis = """
🔍 Key Insights:
• Alice knows Bob and Charlie (2 friends)
• Bob is well-connected (knows Alice, Charlie, and Eve - 3 friends)  
• Charlie is central (knows Alice, Bob, and Diana - 3 friends)
• Diana bridges to Frank (knows Charlie, Eve, and Frank - 3 friends)
• Eve knows Bob and Diana (2 friends)
• Frank is peripheral (only knows Diana - 1 friend)

🎯 Network Properties:
• No isolated nodes (everyone has at least 1 friend)
• Multiple paths between most people
• Diana is a bridge to Frank
• Clustering around Bob-Charlie connection
"""
print(analysis)

print("\n💡 Real-World Applications:")
applications = """
🌐 This same structure represents:
• Social media networks (Facebook, LinkedIn)
• Communication patterns (email, messaging)  
• Web link structures (websites linking to each other)
• Scientific collaboration networks
• Financial transaction networks
• Transportation routes
• Supply chain relationships
"""
print(applications)

print("\n🛠️ How to Fix GraphFrames Setup:")
setup_instructions = """
📋 Complete Setup Instructions:

1️⃣ Install GraphFrames Python package:
   pip install graphframes

2️⃣ Download GraphFrames JAR:
   • Go to: https://spark-packages.org/package/graphframes/graphframes
   • Download JAR for your Spark version
   • Example: graphframes-0.8.2-spark3.2-s_2.12.jar

3️⃣ Method A - Automatic download (what we tried):
   spark = SparkSession.builder \\
       .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \\
       .getOrCreate()

4️⃣ Method B - Local JAR file:
   spark = SparkSession.builder \\
       .config("spark.jars", "/path/to/graphframes-0.8.2-spark3.2-s_2.12.jar") \\
       .getOrCreate()

5️⃣ Method C - Environment variable:
   export PYSPARK_SUBMIT_ARGS="--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell"
"""
print(setup_instructions)

print("\n🎓 What You've Learned:")
learning_summary = """
✅ Graph Fundamentals:
   • Vertices = entities (people, places, things)
   • Edges = relationships (friendships, connections, transactions)
   • Properties = data attached to vertices/edges

✅ Graph Creation:
   • Build vertices DataFrame with IDs and properties
   • Build edges DataFrame with source, destination, properties
   • Combine into graph structure

✅ Basic Analysis:
   • Count vertices and edges
   • Find connections and neighbors
   • Identify important nodes
   • Visualize network structure

✅ Practical Skills:
   • Handle setup issues gracefully
   • Perform analysis with/without specialized libraries
   • Understand real-world applications
"""
print(learning_summary)

print("\n✅ Example 1 complete!")
print("💡 Key takeaway: Graph concepts work whether you use GraphFrames or pure Spark!")
print("🚀 Ready for Example 2? We'll load graph data from CSV files!")

# Clean up
spark.stop()