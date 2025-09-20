import React, { useState, useEffect } from "react";
import {
  Search,
  BookOpen,
  User,
  Calendar,
  ExternalLink,
  Bookmark,
  ChevronDown,
  ChevronUp,
} from "lucide-react";

// Mock data for demonstration
const mockPapers = [
  {
    id: 1,
    title:
      "Deep Learning Approaches for Natural Language Processing in Academic Literature",
    authors: ["Sarah Johnson", "Michael Chen", "David Rodriguez"],
    abstract:
      "This paper presents a comprehensive survey of deep learning techniques applied to natural language processing tasks in academic literature. We explore transformer architectures, attention mechanisms, and their applications in document classification, sentiment analysis, and automatic summarization of research papers.",
    journal: "Journal of Machine Learning Research",
    year: 2023,
    citations: 156,
    doi: "10.1145/3580305.3599830",
    keywords: ["deep learning", "NLP", "transformers", "academic literature"],
    type: "Research Article",
    openAccess: true,
  },
  {
    id: 2,
    title: "Quantum Computing Applications in Cryptographic Security",
    authors: ["Alice Wang", "Robert Thompson"],
    abstract:
      "An investigation into the implications of quantum computing on modern cryptographic systems. This study examines post-quantum cryptography methods and their implementation challenges in real-world security applications.",
    journal: "IEEE Transactions on Quantum Engineering",
    year: 2024,
    citations: 89,
    doi: "10.1109/TQE.2024.3398765",
    keywords: ["quantum computing", "cryptography", "security", "post-quantum"],
    type: "Research Article",
    openAccess: false,
  },
  {
    id: 3,
    title: "Climate Change Impact on Marine Biodiversity: A Meta-Analysis",
    authors: ["Elena Petrov", "James Wilson", "Maria Garcia", "John Smith"],
    abstract:
      "This meta-analysis examines 200+ studies on climate change effects on marine ecosystems. We analyze temperature rise impacts on species distribution, coral reef degradation, and ocean acidification consequences for marine food chains.",
    journal: "Nature Climate Change",
    year: 2023,
    citations: 243,
    doi: "10.1038/s41558-023-01234-5",
    keywords: [
      "climate change",
      "marine biology",
      "biodiversity",
      "meta-analysis",
    ],
    type: "Review Article",
    openAccess: true,
  },
  {
    id: 4,
    title: "Sustainable Urban Planning: Smart Cities and Green Infrastructure",
    authors: ["Thomas Anderson", "Lisa Park"],
    abstract:
      "This paper explores the integration of smart city technologies with green infrastructure in urban planning. We present case studies from five major cities and analyze the effectiveness of sustainable development approaches.",
    journal: "Urban Planning Review",
    year: 2022,
    citations: 178,
    doi: "10.1016/j.upr.2022.05.012",
    keywords: [
      "urban planning",
      "smart cities",
      "sustainability",
      "green infrastructure",
    ],
    type: "Research Article",
    openAccess: true,
  },
  {
    id: 5,
    title:
      "CRISPR-Cas9 Gene Editing: Ethical Considerations and Future Directions",
    authors: ["Dr. Rachel Green", "Prof. Mark Davis"],
    abstract:
      "A comprehensive review of ethical implications surrounding CRISPR-Cas9 gene editing technology. This paper discusses regulatory frameworks, safety concerns, and potential therapeutic applications while addressing societal implications.",
    journal: "Bioethics Quarterly",
    year: 2024,
    citations: 67,
    doi: "10.1111/bioe.13045",
    keywords: ["CRISPR", "gene editing", "bioethics", "genetic engineering"],
    type: "Review Article",
    openAccess: false,
  },
];

const subjects = [
  "Computer Science",
  "Biology",
  "Physics",
  "Chemistry",
  "Mathematics",
  "Engineering",
  "Medicine",
  "Environmental Science",
  "Psychology",
  "Economics",
];

function App() {
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState(mockPapers);
  const [savedPapers, setSavedPapers] = useState(new Set());
  const [sortBy, setSortBy] = useState("relevance");
  const [currentPage, setCurrentPage] = useState(1);
  const [showAdvancedSearch, setShowAdvancedSearch] = useState(false);

  // Filter states
  const [filters, setFilters] = useState({
    yearRange: [2020, 2024],
    subjects: [],
    authors: "",
    paperType: [],
    openAccess: false,
    minCitations: 0,
  });

  // Debounced search
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      handleSearch();
    }, 300);
    return () => clearTimeout(timeoutId);
  }, [searchQuery, filters, sortBy]);

  const handleSearch = () => {
    let results = mockPapers.filter((paper) => {
      // Text search
      const searchLower = searchQuery.toLowerCase();
      const matchesSearch =
        !searchQuery ||
        paper.title.toLowerCase().includes(searchLower) ||
        paper.abstract.toLowerCase().includes(searchLower) ||
        paper.authors.some((author) =>
          author.toLowerCase().includes(searchLower)
        ) ||
        paper.keywords.some((keyword) =>
          keyword.toLowerCase().includes(searchLower)
        );

      // Year filter
      const matchesYear =
        paper.year >= filters.yearRange[0] &&
        paper.year <= filters.yearRange[1];

      // Author filter
      const matchesAuthor =
        !filters.authors ||
        paper.authors.some((author) =>
          author.toLowerCase().includes(filters.authors.toLowerCase())
        );

      // Paper type filter
      const matchesType =
        filters.paperType.length === 0 ||
        filters.paperType.includes(paper.type);

      // Open access filter
      const matchesOpenAccess = !filters.openAccess || paper.openAccess;

      // Citation filter
      const matchesCitations = paper.citations >= filters.minCitations;

      return (
        matchesSearch &&
        matchesYear &&
        matchesAuthor &&
        matchesType &&
        matchesOpenAccess &&
        matchesCitations
      );
    });

    // Sorting
    results.sort((a, b) => {
      switch (sortBy) {
        case "year":
          return b.year - a.year;
        case "citations":
          return b.citations - a.citations;
        case "title":
          return a.title.localeCompare(b.title);
        default:
          return 0;
      }
    });

    setSearchResults(results);
    setCurrentPage(1);
  };

  const toggleSavedPaper = (paperId) => {
    const newSaved = new Set(savedPapers);
    if (newSaved.has(paperId)) {
      newSaved.delete(paperId);
    } else {
      newSaved.add(paperId);
    }
    setSavedPapers(newSaved);
  };

  const updateFilter = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const clearFilters = () => {
    setFilters({
      yearRange: [2020, 2024],
      subjects: [],
      authors: "",
      paperType: [],
      openAccess: false,
      minCitations: 0,
    });
  };

  const resultsPerPage = 10;
  const totalPages = Math.ceil(searchResults.length / resultsPerPage);
  const displayedResults = searchResults.slice(
    (currentPage - 1) * resultsPerPage,
    currentPage * resultsPerPage
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-purple-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-indigo-600 to-purple-600 shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-3">
              <BookOpen className="h-8 w-8 text-white" />
              <span className="text-xl font-bold text-white">
                AcademicSearch
              </span>
            </div>
            <div className="flex items-center space-x-4">
              <button className="text-indigo-100 hover:text-white transition-colors duration-200 px-4 py-2 rounded-md hover:bg-white/10">
                Login
              </button>
              <User className="h-6 w-6 text-indigo-200" />
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Search Section */}
        <div className="mb-8">
          <div className="relative max-w-3xl mx-auto">
            <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 h-5 w-5 text-indigo-400" />
            <input
              type="text"
              placeholder="Search papers, authors, keywords..."
              className="w-full pl-12 pr-4 py-4 text-lg border-2 border-indigo-200 rounded-xl focus:ring-4 focus:ring-indigo-100 focus:border-indigo-500 transition-all duration-200 shadow-lg bg-white"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>

          <div className="flex justify-center mt-6 space-x-4">
            <button
              onClick={() => setShowAdvancedSearch(!showAdvancedSearch)}
              className="text-sm text-indigo-600 hover:text-indigo-800 flex items-center bg-indigo-50 px-4 py-2 rounded-lg hover:bg-indigo-100 transition-colors duration-200 font-medium"
            >
              Advanced Search
              {showAdvancedSearch ? (
                <ChevronUp className="ml-1 h-4 w-4" />
              ) : (
                <ChevronDown className="ml-1 h-4 w-4" />
              )}
            </button>
          </div>

          {/* Advanced Search */}
          {showAdvancedSearch && (
            <div className="max-w-3xl mx-auto mt-6 p-6 bg-white rounded-xl shadow-lg border border-indigo-100">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <input
                  type="text"
                  placeholder="Author name"
                  className="px-4 py-3 border border-indigo-200 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all duration-200"
                  value={filters.authors}
                  onChange={(e) => updateFilter("authors", e.target.value)}
                />
                <select className="px-4 py-3 border border-indigo-200 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all duration-200">
                  <option>All subjects</option>
                  {subjects.map((subject) => (
                    <option key={subject}>{subject}</option>
                  ))}
                </select>
              </div>
            </div>
          )}
        </div>

        {/* Results Section */}
        <div className="flex flex-col lg:flex-row gap-8">
          {/* Filters Sidebar */}
          <div className="lg:w-64 flex-shrink-0">
            <div className="bg-white rounded-xl shadow-lg border border-slate-200 p-6 backdrop-blur-sm">
              <div className="flex items-center justify-between mb-6">
                <h3 className="font-bold text-slate-800 text-lg">Filters</h3>
                <button
                  onClick={clearFilters}
                  className="text-sm text-rose-600 hover:text-rose-800 font-medium bg-rose-50 px-3 py-1 rounded-md hover:bg-rose-100 transition-colors duration-200"
                >
                  Clear all
                </button>
              </div>

              {/* Year Range */}
              <div className="mb-6">
                <label className="block text-sm font-semibold text-slate-700 mb-3">
                  Publication Year
                </label>
                <div className="flex items-center space-x-3">
                  <input
                    type="number"
                    min="1990"
                    max="2024"
                    className="w-20 px-3 py-2 border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all duration-200"
                    value={filters.yearRange[0]}
                    onChange={(e) =>
                      updateFilter("yearRange", [
                        parseInt(e.target.value),
                        filters.yearRange[1],
                      ])
                    }
                  />
                  <span className="text-slate-500 font-medium">to</span>
                  <input
                    type="number"
                    min="1990"
                    max="2024"
                    className="w-20 px-3 py-2 border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all duration-200"
                    value={filters.yearRange[1]}
                    onChange={(e) =>
                      updateFilter("yearRange", [
                        filters.yearRange[0],
                        parseInt(e.target.value),
                      ])
                    }
                  />
                </div>
              </div>

              {/* Paper Type */}
              <div className="mb-6">
                <label className="block text-sm font-semibold text-slate-700 mb-3">
                  Paper Type
                </label>
                <div className="space-y-3">
                  {[
                    "Research Article",
                    "Review Article",
                    "Conference Paper",
                  ].map((type) => (
                    <label key={type} className="flex items-center group">
                      <input
                        type="checkbox"
                        className="mr-3 text-indigo-600 focus:ring-indigo-500 focus:ring-2 rounded transition-all duration-200"
                        checked={filters.paperType.includes(type)}
                        onChange={(e) => {
                          const newTypes = e.target.checked
                            ? [...filters.paperType, type]
                            : filters.paperType.filter((t) => t !== type);
                          updateFilter("paperType", newTypes);
                        }}
                      />
                      <span className="text-sm text-slate-700 group-hover:text-slate-900 transition-colors duration-200">
                        {type}
                      </span>
                    </label>
                  ))}
                </div>
              </div>

              {/* Open Access */}
              <div className="mb-6">
                <label className="flex items-center group">
                  <input
                    type="checkbox"
                    className="mr-3 text-emerald-600 focus:ring-emerald-500 focus:ring-2 rounded transition-all duration-200"
                    checked={filters.openAccess}
                    onChange={(e) =>
                      updateFilter("openAccess", e.target.checked)
                    }
                  />
                  <span className="text-sm text-slate-700 group-hover:text-slate-900 transition-colors duration-200 font-medium">
                    Open Access Only
                  </span>
                </label>
              </div>

              {/* Minimum Citations */}
              <div>
                <label className="block text-sm font-semibold text-slate-700 mb-3">
                  Minimum Citations
                </label>
                <input
                  type="number"
                  min="0"
                  className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition-all duration-200"
                  value={filters.minCitations}
                  onChange={(e) =>
                    updateFilter("minCitations", parseInt(e.target.value) || 0)
                  }
                />
              </div>
            </div>
          </div>

          {/* Results */}
          <div className="flex-1">
            {/* Results Header */}
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-8">
              <p className="text-slate-600 font-medium">
                {searchResults.length.toLocaleString()} results
                {searchQuery && ` for "${searchQuery}"`}
              </p>
              <div className="flex items-center space-x-3 mt-2 sm:mt-0">
                <span className="text-sm text-slate-700 font-medium">
                  Sort by:
                </span>
                <select
                  className="px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 bg-white text-slate-700 font-medium transition-all duration-200"
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                >
                  <option value="relevance">Relevance</option>
                  <option value="year">Year</option>
                  <option value="citations">Citations</option>
                  <option value="title">Title</option>
                </select>
              </div>
            </div>

            {/* Paper Cards */}
            <div className="space-y-6">
              {displayedResults.map((paper) => (
                <PaperCard
                  key={paper.id}
                  paper={paper}
                  isSaved={savedPapers.has(paper.id)}
                  onToggleSave={() => toggleSavedPaper(paper.id)}
                />
              ))}
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex justify-center items-center space-x-3 mt-10">
                <button
                  onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                  disabled={currentPage === 1}
                  className="px-4 py-2 border border-slate-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50 text-slate-700 font-medium transition-all duration-200 hover:border-slate-400"
                >
                  Previous
                </button>

                <div className="flex space-x-1">
                  {[...Array(Math.min(5, totalPages))].map((_, i) => {
                    const page = i + 1;
                    return (
                      <button
                        key={page}
                        onClick={() => setCurrentPage(page)}
                        className={`px-4 py-2 border rounded-lg font-medium transition-all duration-200 ${
                          currentPage === page
                            ? "bg-gradient-to-r from-indigo-600 to-purple-600 text-white border-indigo-600 shadow-md"
                            : "border-slate-300 hover:bg-slate-50 text-slate-700 hover:border-slate-400"
                        }`}
                      >
                        {page}
                      </button>
                    );
                  })}
                </div>

                <button
                  onClick={() =>
                    setCurrentPage(Math.min(totalPages, currentPage + 1))
                  }
                  disabled={currentPage === totalPages}
                  className="px-4 py-2 border border-slate-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50 text-slate-700 font-medium transition-all duration-200 hover:border-slate-400"
                >
                  Next
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function PaperCard({ paper, isSaved, onToggleSave }) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div className="bg-white rounded-xl shadow-lg border border-slate-200 p-8 hover:shadow-xl transition-all duration-300 hover:border-indigo-200 group">
      {/* Header */}
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-xl font-bold text-slate-800 hover:text-indigo-600 cursor-pointer flex-1 mr-4 leading-tight group-hover:text-indigo-700 transition-colors duration-200">
          {paper.title}
        </h3>
        <button
          onClick={onToggleSave}
          className={`p-2 rounded-full hover:bg-slate-100 transition-all duration-200 ${
            isSaved
              ? "text-rose-600 bg-rose-50"
              : "text-slate-400 hover:text-rose-500"
          }`}
        >
          <Bookmark className={`h-5 w-5 ${isSaved ? "fill-current" : ""}`} />
        </button>
      </div>

      {/* Authors */}
      <div className="flex flex-wrap items-center text-sm text-slate-600 mb-4">
        {paper.authors.map((author, index) => (
          <span key={author}>
            <span className="hover:text-indigo-600 cursor-pointer font-medium transition-colors duration-200">
              {author}
            </span>
            {index < paper.authors.length - 1 && (
              <span className="mx-2 text-slate-400">•</span>
            )}
          </span>
        ))}
      </div>

      {/* Abstract */}
      <p className="text-slate-700 mb-6 leading-relaxed">
        {isExpanded ? paper.abstract : `${paper.abstract.substring(0, 200)}...`}
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="ml-2 text-indigo-600 hover:text-indigo-800 text-sm font-semibold transition-colors duration-200"
        >
          {isExpanded ? "Show less" : "Show more"}
        </button>
      </p>

      {/* Metadata */}
      <div className="flex flex-wrap items-center justify-between text-sm text-slate-600 mb-4">
        <div className="flex items-center space-x-4">
          <span className="flex items-center bg-slate-50 px-3 py-1 rounded-full">
            <Calendar className="h-4 w-4 mr-2 text-slate-500" />
            {paper.journal}, {paper.year}
          </span>
          <span className="bg-amber-50 text-amber-700 px-3 py-1 rounded-full font-medium">
            {paper.citations} citations
          </span>
          {paper.openAccess && (
            <span className="bg-emerald-100 text-emerald-800 px-3 py-1 rounded-full text-xs font-semibold">
              🔓 Open Access
            </span>
          )}
        </div>
        <span className="text-indigo-600 font-semibold bg-indigo-50 px-3 py-1 rounded-full">
          {paper.type}
        </span>
      </div>

      {/* Keywords */}
      <div className="flex flex-wrap gap-2 mb-6">
        {paper.keywords.map((keyword) => (
          <span
            key={keyword}
            className="bg-gradient-to-r from-purple-100 to-indigo-100 text-purple-700 px-3 py-1 rounded-full text-sm hover:from-purple-200 hover:to-indigo-200 cursor-pointer transition-all duration-200 font-medium"
          >
            #{keyword}
          </span>
        ))}
      </div>

      {/* Actions */}
      <div className="flex items-center justify-between pt-4 border-t border-slate-100">
        <a
          href={`https://doi.org/${paper.doi}`}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-4 py-2 rounded-lg hover:from-indigo-700 hover:to-purple-700 text-sm font-semibold transition-all duration-200 shadow-md hover:shadow-lg"
        >
          View Paper
          <ExternalLink className="h-4 w-4 ml-2" />
        </a>
        <div className="flex space-x-6 text-sm text-slate-600">
          <button className="hover:text-indigo-600 font-medium transition-colors duration-200">
            Cite
          </button>
          <button className="hover:text-indigo-600 font-medium transition-colors duration-200">
            Similar
          </button>
          <button className="hover:text-indigo-600 font-medium transition-colors duration-200">
            Share
          </button>
        </div>
      </div>
    </div>
  );
}

export default App;
