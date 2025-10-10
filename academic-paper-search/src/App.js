import React, { useState, useEffect, useRef } from "react";
import {
  Search,
  BookOpen,
  User,
  Calendar,
  ExternalLink,
  Bookmark,
  ChevronDown,
  ChevronUp,
  Loader2,
  AlertCircle,
  FileText,
  Users,
  TrendingUp,
  ArrowLeft,
  Sparkles,
  Award,
  BarChart3,
  Target,
  Cloud,
} from "lucide-react";

const SEARCH_API_URL = "http://172.17.16.14:8000";
const PAPER_API_URL = "http://172.17.16.14:8000";

function WordCloud({ titles }) {
  const canvasRef = useRef(null);

  useEffect(() => {
    if (!titles || titles.length === 0 || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const width = canvas.width;
    const height = canvas.height;

    // Clear canvas
    ctx.clearRect(0, 0, width, height);

    // Process titles to extract words
    const stopWords = new Set([
      "a",
      "an",
      "and",
      "are",
      "as",
      "at",
      "be",
      "by",
      "for",
      "from",
      "has",
      "he",
      "in",
      "is",
      "it",
      "its",
      "of",
      "on",
      "that",
      "the",
      "to",
      "was",
      "will",
      "with",
      "via",
      "using",
      "based",
      "new",
      "novel",
      "study",
      "analysis",
      "approach",
    ]);

    const wordFreq = {};

    titles.forEach((title) => {
      if (!title) return;
      const words = title
        .toLowerCase()
        .replace(/[^\w\s-]/g, " ")
        .split(/\s+/)
        .filter((word) => word.length > 3 && !stopWords.has(word));

      words.forEach((word) => {
        wordFreq[word] = (wordFreq[word] || 0) + 1;
      });
    });

    // Sort words by frequency
    const sortedWords = Object.entries(wordFreq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 80); // Top 80 words

    if (sortedWords.length === 0) return;

    const maxFreq = sortedWords[0][1];
    const minFreq = sortedWords[sortedWords.length - 1][1];

    // Generate word positions
    const words = sortedWords.map(([word, freq]) => {
      const normalizedFreq = (freq - minFreq) / (maxFreq - minFreq);
      const fontSize = Math.max(14, Math.min(80, 14 + normalizedFreq * 66));
      return { word, freq, fontSize };
    });

    // Place words
    const placed = [];
    const maxAttempts = 50;

    words.forEach((wordData) => {
      ctx.font = `bold ${wordData.fontSize}px Arial`;
      const metrics = ctx.measureText(wordData.word);
      const wordWidth = metrics.width;
      const wordHeight = wordData.fontSize;

      let positioned = false;

      for (let attempt = 0; attempt < maxAttempts && !positioned; attempt++) {
        const x = Math.random() * (width - wordWidth - 20) + 10;
        const y = Math.random() * (height - wordHeight - 20) + wordHeight;

        // Check collision
        const collision = placed.some((p) => {
          return !(
            x + wordWidth < p.x - 5 ||
            x > p.x + p.width + 5 ||
            y - wordHeight > p.y + 5 ||
            y < p.y - p.height - 5
          );
        });

        if (!collision) {
          placed.push({
            x,
            y,
            width: wordWidth,
            height: wordHeight,
            ...wordData,
          });
          positioned = true;
        }
      }
    });

    // Draw words
    placed.forEach((word) => {
      const hue = (word.freq / maxFreq) * 270; // Blue to purple gradient
      ctx.fillStyle = `hsl(${220 + hue}, 70%, ${45 + Math.random() * 20}%)`;
      ctx.font = `bold ${word.fontSize}px Arial`;
      ctx.fillText(word.word, word.x, word.y);
    });
  }, [titles]);

  return (
    <div className="bg-white rounded-xl shadow-lg border-2 border-indigo-100 p-6 mb-6">
      <div className="flex items-center mb-4">
        <Cloud className="h-6 w-6 text-indigo-600 mr-2" />
        <h3 className="text-xl font-bold text-slate-800">Title Word Cloud</h3>
        <span className="ml-3 text-sm text-slate-600">
          ({titles.length} papers analyzed)
        </span>
      </div>
      <div className="bg-gradient-to-br from-indigo-50 to-purple-50 rounded-lg p-4">
        <canvas ref={canvasRef} width={900} height={500} className="w-full" />
      </div>
      <p className="text-xs text-slate-600 mt-3 text-center">
        Larger words appear more frequently in paper titles. Common words are
        filtered out.
      </p>
    </div>
  );
}

function SearchResultCard({ result, onViewDetails, enrichment }) {
  const getPercentileColor = (percentile) => {
    if (percentile >= 90) return "from-emerald-500 to-green-500";
    if (percentile >= 75) return "from-blue-500 to-cyan-500";
    if (percentile >= 50) return "from-amber-500 to-yellow-500";
    return "from-slate-400 to-slate-500";
  };

  return (
    <div className="bg-white rounded-xl shadow-md hover:shadow-xl border-2 border-slate-100 hover:border-indigo-200 transition-all duration-200 p-6 group">
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <div className="flex items-center space-x-3 mb-3 flex-wrap">
            <span className="bg-gradient-to-r from-indigo-600 to-purple-600 text-white text-sm font-bold px-3 py-1 rounded-full">
              #{result.rank}
            </span>
            <span className="text-xs text-slate-500 font-medium">
              Distance: {result.approximate_distance.toFixed(4)}
            </span>
            {enrichment?.fwci !== undefined && enrichment?.fwci !== null && (
              <span className="bg-gradient-to-r from-indigo-50 to-purple-50 text-indigo-700 text-xs font-bold px-3 py-1 rounded-full border border-indigo-200">
                FWCI: {enrichment.fwci.toFixed(2)}
              </span>
            )}
            {enrichment?.citation_percentile !== undefined &&
              enrichment?.citation_percentile !== null && (
                <span
                  className={`bg-gradient-to-r ${getPercentileColor(
                    enrichment.citation_percentile
                  )} text-white text-xs font-bold px-3 py-1 rounded-full`}
                >
                  Top {(100 - enrichment.citation_percentile).toFixed(0)}%
                </span>
              )}
          </div>

          <h3 className="text-xl font-bold text-slate-800 mb-3 group-hover:text-indigo-600 transition-colors duration-200">
            {result.title}
          </h3>

          <div className="flex flex-wrap gap-4 text-sm text-slate-600">
            <div className="flex items-center">
              <FileText className="h-4 w-4 mr-1 text-slate-400" />
              <span className="font-medium">Paper ID:</span>
              <button
                onClick={() => onViewDetails(result.corpus_id)}
                className="ml-1 text-indigo-600 hover:text-indigo-800 font-semibold hover:underline"
              >
                {result.paper_id}
              </button>
            </div>
            <div className="flex items-center">
              <BookOpen className="h-4 w-4 mr-1 text-slate-400" />
              <span className="font-medium">Corpus ID:</span>
              <button
                onClick={() => onViewDetails(result.corpus_id)}
                className="ml-1 text-indigo-600 hover:text-indigo-800 font-semibold hover:underline"
              >
                {result.corpus_id || "N/A"}
              </button>
            </div>
          </div>
        </div>

        <button
          onClick={() => onViewDetails(result.corpus_id)}
          className="ml-4 bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-6 py-3 rounded-lg hover:from-indigo-700 hover:to-purple-700 font-semibold transition-all duration-200 shadow-md hover:shadow-lg flex items-center whitespace-nowrap"
        >
          View Details
          <ExternalLink className="h-4 w-4 ml-2" />
        </button>
      </div>
    </div>
  );
}

function PaperDetailCard({
  paper,
  analytics,
  isSaved,
  onToggleSave,
  showAdvancedInfo,
  setShowAdvancedInfo,
  showFullAnalytics,
  setShowFullAnalytics,
}) {
  const authorNames = paper.authors?.map((a) => a.name).filter(Boolean) || [];
  const paperLink = paper.externalids?.DOI
    ? `https://doi.org/${paper.externalids.DOI}`
    : paper.url || "#";
  const fields =
    paper.s2fieldsofstudy?.map((f) => f.category).filter(Boolean) || [];

  const getPercentileColor = (percentile) => {
    if (percentile >= 90) return "from-emerald-500 to-green-500";
    if (percentile >= 75) return "from-blue-500 to-cyan-500";
    if (percentile >= 50) return "from-amber-500 to-yellow-500";
    return "from-slate-400 to-slate-500";
  };

  const getPercentileLabel = (percentile) => {
    if (percentile >= 90) return "Top 10%";
    if (percentile >= 75) return "Top 25%";
    if (percentile >= 50) return "Top 50%";
    return "Bottom 50%";
  };

  return (
    <div className="bg-white rounded-2xl shadow-2xl border-2 border-indigo-100 overflow-hidden">
      {/* Header Section */}
      <div className="bg-gradient-to-r from-indigo-600 via-purple-600 to-pink-600 px-8 py-8">
        <div className="flex justify-between items-start">
          <div className="flex-1 mr-4">
            <h1 className="text-4xl font-bold text-white leading-tight mb-3">
              {paper.title || "Untitled Paper"}
            </h1>
            {paper.corpusid && (
              <p className="text-indigo-100 text-sm font-medium">
                Corpus ID: {paper.corpusid}
              </p>
            )}
          </div>
          <button
            onClick={onToggleSave}
            className={`p-3 rounded-full transition-all duration-200 flex-shrink-0 ${
              isSaved
                ? "text-rose-500 bg-white"
                : "text-white hover:bg-white/20"
            }`}
          >
            <Bookmark className={`h-6 w-6 ${isSaved ? "fill-current" : ""}`} />
          </button>
        </div>
      </div>

      {/* Analytics Banner */}
      {analytics && (
        <div className="bg-gradient-to-r from-purple-50 to-indigo-50 px-8 py-6 border-b-2 border-indigo-100">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <Award className="h-6 w-6 text-indigo-600 mr-2" />
              <h3 className="text-xl font-bold text-slate-800">
                Research Impact Metrics
              </h3>
            </div>
            <button
              onClick={() => setShowFullAnalytics(!showFullAnalytics)}
              className="bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-6 py-2 rounded-lg hover:from-indigo-700 hover:to-purple-700 font-semibold transition-all duration-200 shadow-md hover:shadow-lg flex items-center text-sm"
            >
              <BarChart3 className="h-4 w-4 mr-2" />
              {showFullAnalytics ? "Hide" : "See"} Full Analytics
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* FWCI Card */}
            <div className="bg-white rounded-xl p-5 shadow-md border-2 border-indigo-100">
              <div className="flex items-center mb-2">
                <Target className="h-5 w-5 text-indigo-600 mr-2" />
                <span className="text-sm font-semibold text-slate-600">
                  Field-Weighted Citation Impact
                </span>
              </div>
              <div className="text-3xl font-bold text-indigo-600 mb-1">
                {analytics.fwci?.toFixed(2) || "N/A"}
              </div>
              <p className="text-xs text-slate-600">
                {analytics.fwci >= 1
                  ? `${((analytics.fwci - 1) * 100).toFixed(
                      0
                    )}% above field average`
                  : `${((1 - analytics.fwci) * 100).toFixed(
                      0
                    )}% below field average`}
              </p>
            </div>

            {/* Citation Percentile Card */}
            <div className="bg-white rounded-xl p-5 shadow-md border-2 border-indigo-100">
              <div className="flex items-center mb-2">
                <TrendingUp className="h-5 w-5 text-purple-600 mr-2" />
                <span className="text-sm font-semibold text-slate-600">
                  Citation Percentile
                </span>
              </div>
              <div
                className={`text-3xl font-bold bg-gradient-to-r ${getPercentileColor(
                  analytics.citation_percentile
                )} bg-clip-text text-transparent mb-1`}
              >
                {analytics.citation_percentile?.toFixed(1) || "N/A"}%
              </div>
              <p className="text-xs text-slate-600">
                {getPercentileLabel(analytics.citation_percentile)} in field
              </p>
            </div>

            {/* Field ID Card */}
            <div className="bg-white rounded-xl p-5 shadow-md border-2 border-indigo-100">
              <div className="flex items-center mb-2">
                <BookOpen className="h-5 w-5 text-pink-600 mr-2" />
                <span className="text-sm font-semibold text-slate-600">
                  Research Field ID
                </span>
              </div>
              <div className="text-3xl font-bold text-pink-600 mb-1">
                {analytics.field_id || "N/A"}
              </div>
              <p className="text-xs text-slate-600">
                Field classification cluster
              </p>
            </div>
          </div>

          {/* Full Analytics Details */}
          {showFullAnalytics && (
            <div className="mt-6 bg-white rounded-xl p-6 shadow-md border-2 border-indigo-100">
              <h4 className="text-lg font-bold text-slate-800 mb-4">
                Understanding Your Metrics
              </h4>
              <div className="space-y-4 text-sm text-slate-700">
                <div className="flex items-start">
                  <div className="bg-indigo-100 rounded-full p-2 mr-3 mt-0.5">
                    <Target className="h-4 w-4 text-indigo-600" />
                  </div>
                  <div>
                    <p className="font-semibold text-slate-800 mb-1">
                      Field-Weighted Citation Impact (FWCI):{" "}
                      {analytics.fwci?.toFixed(2)}
                    </p>
                    <p className="text-slate-600">
                      Compares citation performance against global average for
                      similar papers in the same field and publication year. A
                      value of 1.0 means performance is exactly at the world
                      average. Values above 1.0 indicate above-average impact.
                    </p>
                  </div>
                </div>
                <div className="flex items-start">
                  <div className="bg-purple-100 rounded-full p-2 mr-3 mt-0.5">
                    <TrendingUp className="h-4 w-4 text-purple-600" />
                  </div>
                  <div>
                    <p className="font-semibold text-slate-800 mb-1">
                      Citation Percentile:{" "}
                      {analytics.citation_percentile?.toFixed(1)}%
                    </p>
                    <p className="text-slate-600">
                      Shows what percentage of papers in the same field this
                      paper outperforms in terms of citations. A percentile of
                      90% means this paper has more citations than 90% of
                      similar papers.
                    </p>
                  </div>
                </div>
                <div className="flex items-start">
                  <div className="bg-pink-100 rounded-full p-2 mr-3 mt-0.5">
                    <BookOpen className="h-4 w-4 text-pink-600" />
                  </div>
                  <div>
                    <p className="font-semibold text-slate-800 mb-1">
                      Field ID: {analytics.field_id}
                    </p>
                    <p className="text-slate-600">
                      Unique identifier for the research field cluster this
                      paper belongs to. Papers with the same field ID are
                      compared against each other for impact metrics.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Main Content */}
      <div className="p-8">
        {/* Authors Section */}
        {authorNames.length > 0 && (
          <div className="mb-6 pb-6 border-b border-slate-200">
            <div className="flex items-center mb-3">
              <Users className="h-5 w-5 text-indigo-600 mr-2" />
              <h3 className="text-lg font-semibold text-slate-800">Authors</h3>
            </div>
            <div className="flex flex-wrap gap-2">
              {authorNames.map((author, index) => (
                <span
                  key={index}
                  className="bg-gradient-to-r from-indigo-50 to-purple-50 text-indigo-700 px-4 py-2 rounded-lg font-medium hover:from-indigo-100 hover:to-purple-100 transition-all duration-200 cursor-pointer shadow-sm"
                >
                  {author}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Key Metrics */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 pb-6 border-b border-slate-200">
          {paper.year && (
            <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-xl p-4 text-center shadow-sm">
              <Calendar className="h-6 w-6 text-blue-600 mx-auto mb-2" />
              <p className="text-2xl font-bold text-blue-800">{paper.year}</p>
              <p className="text-xs text-blue-700 mt-1 font-medium">Year</p>
            </div>
          )}
          {paper.citationcount !== undefined &&
            paper.citationcount !== null && (
              <div className="bg-gradient-to-br from-amber-50 to-amber-100 rounded-xl p-4 text-center shadow-sm">
                <FileText className="h-6 w-6 text-amber-600 mx-auto mb-2" />
                <p className="text-2xl font-bold text-amber-800">
                  {paper.citationcount.toLocaleString()}
                </p>
                <p className="text-xs text-amber-700 mt-1 font-medium">
                  Citations
                </p>
              </div>
            )}
          {paper.influentialcitationcount !== undefined &&
            paper.influentialcitationcount !== null && (
              <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-xl p-4 text-center shadow-sm">
                <TrendingUp className="h-6 w-6 text-purple-600 mx-auto mb-2" />
                <p className="text-2xl font-bold text-purple-800">
                  {paper.influentialcitationcount.toLocaleString()}
                </p>
                <p className="text-xs text-purple-700 mt-1 font-medium">
                  Influential
                </p>
              </div>
            )}
          {paper.referencecount !== undefined &&
            paper.referencecount !== null && (
              <div className="bg-gradient-to-br from-emerald-50 to-emerald-100 rounded-xl p-4 text-center shadow-sm">
                <FileText className="h-6 w-6 text-emerald-600 mx-auto mb-2" />
                <p className="text-2xl font-bold text-emerald-800">
                  {paper.referencecount.toLocaleString()}
                </p>
                <p className="text-xs text-emerald-700 mt-1 font-medium">
                  References
                </p>
              </div>
            )}
        </div>

        {/* Publication Info */}
        <div className="mb-6 pb-6 border-b border-slate-200">
          <h3 className="text-lg font-semibold text-slate-800 mb-4">
            Publication Information
          </h3>
          <div className="space-y-3">
            {paper.venue && (
              <div className="flex items-start">
                <span className="text-slate-600 font-medium min-w-[140px]">
                  Venue:
                </span>
                <span className="text-slate-800">{paper.venue}</span>
              </div>
            )}
            {paper.journal?.name && (
              <div className="flex items-start">
                <span className="text-slate-600 font-medium min-w-[140px]">
                  Journal:
                </span>
                <span className="text-slate-800">
                  {paper.journal.name}
                  {paper.journal.volume && ` (Vol. ${paper.journal.volume})`}
                  {paper.journal.pages && `, Pages: ${paper.journal.pages}`}
                </span>
              </div>
            )}
            {paper.publicationdate && (
              <div className="flex items-start">
                <span className="text-slate-600 font-medium min-w-[140px]">
                  Publication Date:
                </span>
                <span className="text-slate-800">{paper.publicationdate}</span>
              </div>
            )}
            {paper.publicationtypes && paper.publicationtypes.length > 0 && (
              <div className="flex items-start">
                <span className="text-slate-600 font-medium min-w-[140px]">
                  Type:
                </span>
                <div className="flex flex-wrap gap-2">
                  {paper.publicationtypes.map((type, index) => (
                    <span
                      key={index}
                      className="bg-slate-100 text-slate-700 px-3 py-1 rounded-full text-sm font-medium"
                    >
                      {type}
                    </span>
                  ))}
                </div>
              </div>
            )}
            {paper.isopenaccess !== undefined && (
              <div className="flex items-start">
                <span className="text-slate-600 font-medium min-w-[140px]">
                  Access:
                </span>
                <span
                  className={`px-3 py-1 rounded-full text-sm font-semibold ${
                    paper.isopenaccess
                      ? "bg-emerald-100 text-emerald-800"
                      : "bg-slate-100 text-slate-700"
                  }`}
                >
                  {paper.isopenaccess ? "🔓 Open Access" : "🔒 Restricted"}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Fields of Study */}
        {fields.length > 0 && (
          <div className="mb-6 pb-6 border-b border-slate-200">
            <h3 className="text-lg font-semibold text-slate-800 mb-4">
              Fields of Study
            </h3>
            <div className="flex flex-wrap gap-2">
              {fields.map((field, index) => (
                <span
                  key={index}
                  className="bg-gradient-to-r from-purple-100 to-indigo-100 text-purple-700 px-4 py-2 rounded-full text-sm hover:from-purple-200 hover:to-indigo-200 cursor-pointer transition-all duration-200 font-medium shadow-sm"
                >
                  #{field}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Advanced Information Toggle */}
        <div className="mb-6">
          <button
            onClick={() => setShowAdvancedInfo(!showAdvancedInfo)}
            className="flex items-center text-indigo-600 hover:text-indigo-800 font-semibold transition-colors duration-200"
          >
            {showAdvancedInfo ? "Hide" : "Show"} Advanced Information
            {showAdvancedInfo ? (
              <ChevronUp className="ml-2 h-5 w-5" />
            ) : (
              <ChevronDown className="ml-2 h-5 w-5" />
            )}
          </button>
        </div>

        {/* Advanced Information */}
        {showAdvancedInfo && (
          <div className="bg-gradient-to-br from-slate-50 to-slate-100 rounded-xl p-6 mb-6 shadow-inner">
            <h3 className="text-lg font-semibold text-slate-800 mb-4">
              External Identifiers
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
              {paper.corpusid && (
                <div>
                  <span className="text-slate-600 font-medium">Corpus ID:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.corpusid}
                  </span>
                </div>
              )}
              {paper.externalids?.DOI && (
                <div>
                  <span className="text-slate-600 font-medium">DOI:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.DOI}
                  </span>
                </div>
              )}
              {paper.externalids?.ArXiv && (
                <div>
                  <span className="text-slate-600 font-medium">ArXiv:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.ArXiv}
                  </span>
                </div>
              )}
              {paper.externalids?.PubMed && (
                <div>
                  <span className="text-slate-600 font-medium">PubMed:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.PubMed}
                  </span>
                </div>
              )}
              {paper.externalids?.PubMedCentral && (
                <div>
                  <span className="text-slate-600 font-medium">PMC:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.PubMedCentral}
                  </span>
                </div>
              )}
              {paper.externalids?.DBLP && (
                <div>
                  <span className="text-slate-600 font-medium">DBLP:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.DBLP}
                  </span>
                </div>
              )}
              {paper.externalids?.MAG && (
                <div>
                  <span className="text-slate-600 font-medium">MAG:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.MAG}
                  </span>
                </div>
              )}
              {paper.externalids?.ACL && (
                <div>
                  <span className="text-slate-600 font-medium">ACL:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.externalids.ACL}
                  </span>
                </div>
              )}
              {paper.publicationvenueid && (
                <div>
                  <span className="text-slate-600 font-medium">Venue ID:</span>{" "}
                  <span className="text-slate-800 font-mono">
                    {paper.publicationvenueid}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex flex-col sm:flex-row items-center justify-between gap-4 pt-6 border-t border-slate-200">
          <a
            href={paperLink}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-8 py-3 rounded-xl hover:from-indigo-700 hover:to-purple-700 font-semibold transition-all duration-200 shadow-lg hover:shadow-xl"
          >
            View Full Paper
            <ExternalLink className="h-5 w-5 ml-2" />
          </a>
          <div className="flex space-x-6 text-sm text-slate-600">
            <button className="hover:text-indigo-600 font-medium transition-colors duration-200 flex items-center">
              <FileText className="h-4 w-4 mr-1" />
              Cite
            </button>
            <button className="hover:text-indigo-600 font-medium transition-colors duration-200 flex items-center">
              <Search className="h-4 w-4 mr-1" />
              Similar
            </button>
            <button className="hover:text-indigo-600 font-medium transition-colors duration-200 flex items-center">
              <ExternalLink className="h-4 w-4 mr-1" />
              Share
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [view, setView] = useState("search");
  const [searchQuery, setSearchQuery] = useState("");
  const [nprobe, setNprobe] = useState(64);
  const [searchResults, setSearchResults] = useState([]);
  const [enrichedResults, setEnrichedResults] = useState([]);
  const [paperData, setPaperData] = useState(null);
  const [analyticsData, setAnalyticsData] = useState(null);
  const [savedPapers, setSavedPapers] = useState(new Set());
  const [isSearching, setIsSearching] = useState(false);
  const [isEnriching, setIsEnriching] = useState(false);
  const [isFetchingPaper, setIsFetchingPaper] = useState(false);
  const [error, setError] = useState(null);
  const [showAdvancedInfo, setShowAdvancedInfo] = useState(false);
  const [showAdvancedSearch, setShowAdvancedSearch] = useState(false);
  const [showFullAnalytics, setShowFullAnalytics] = useState(false);

  const performSearch = async () => {
    if (!searchQuery.trim()) {
      setError("Please enter a search query");
      return;
    }

    setIsSearching(true);
    setError(null);
    setSearchResults([]);
    setEnrichedResults([]);

    try {
      const response = await fetch(`${SEARCH_API_URL}/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query_text: searchQuery,
          nprobe: nprobe,
        }),
      });

      if (!response.ok) {
        throw new Error(
          `Search failed: ${response.status} ${response.statusText}`
        );
      }

      const data = await response.json();

      // Remove duplicates based on corpus_id
      const uniqueResults = [];
      const seenCorpusIds = new Set();

      for (const result of data) {
        if (result.corpus_id && !seenCorpusIds.has(result.corpus_id)) {
          seenCorpusIds.add(result.corpus_id);
          uniqueResults.push(result);
        }
      }

      setSearchResults(uniqueResults);

      // Collect corpus IDs and enrich results
      const corpusIds = uniqueResults
        .map((result) => result.corpus_id)
        .filter(Boolean);

      if (corpusIds.length > 0) {
        setIsEnriching(true);
        try {
          const enrichResponse = await fetch(`${PAPER_API_URL}/enrich`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ corpus_ids: corpusIds }),
          });

          if (enrichResponse.ok) {
            const enrichData = await enrichResponse.json();

            // Create a map of corpus_id to enrichment data
            const enrichmentMap = new Map();
            enrichData.forEach((item) => {
              if (item.corpus_id) {
                enrichmentMap.set(item.corpus_id, item);
              }
            });

            // Merge enrichment data with search results
            const mergedResults = uniqueResults.map((result) => ({
              ...result,
              enrichment: enrichmentMap.get(result.corpus_id) || null,
            }));

            // Sort by FWCI (descending), putting papers without FWCI at the end
            const sortedResults = mergedResults.sort((a, b) => {
              const fwciA = a.enrichment?.fwci ?? -Infinity;
              const fwciB = b.enrichment?.fwci ?? -Infinity;
              return fwciB - fwciA;
            });

            setEnrichedResults(sortedResults);
          } else {
            // If enrichment fails, use original results
            setEnrichedResults(
              uniqueResults.map((r) => ({ ...r, enrichment: null }))
            );
          }
        } catch (enrichErr) {
          console.error("Enrichment failed:", enrichErr);
          // If enrichment fails, use original results
          setEnrichedResults(
            uniqueResults.map((r) => ({ ...r, enrichment: null }))
          );
        } finally {
          setIsEnriching(false);
        }
      }
    } catch (err) {
      setError(err.message || "Failed to perform search");
    } finally {
      setIsSearching(false);
    }
  };

  const fetchPaperDetails = async (corpusId) => {
    setIsFetchingPaper(true);
    setError(null);
    setPaperData(null);
    setAnalyticsData(null);
    setView("details");

    try {
      // Fetch both paper details and analytics simultaneously
      const [paperResponse, analyticsResponse] = await Promise.all([
        fetch(`${PAPER_API_URL}/paper/${corpusId}`),
        fetch(`${PAPER_API_URL}/enrich`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ corpus_ids: [corpusId] }),
        }),
      ]);

      // Handle paper details response
      if (!paperResponse.ok) {
        if (paperResponse.status === 404) {
          throw new Error(`Paper with Corpus ID ${corpusId} not found`);
        } else if (paperResponse.status === 500) {
          throw new Error("Spark job failed. Please check server logs.");
        } else if (paperResponse.status === 504) {
          throw new Error("Request timed out. Please try again.");
        } else {
          throw new Error(
            `Error: ${paperResponse.status} ${paperResponse.statusText}`
          );
        }
      }

      const paperData = await paperResponse.json();
      setPaperData(paperData);

      // Handle analytics response
      if (analyticsResponse.ok) {
        const analyticsData = await analyticsResponse.json();
        if (analyticsData && analyticsData.length > 0) {
          setAnalyticsData(analyticsData[0]);
        }
      }

      setShowAdvancedInfo(false);
    } catch (err) {
      setError(err.message || "Failed to fetch paper details");
      setPaperData(null);
      setAnalyticsData(null);
    } finally {
      setIsFetchingPaper(false);
    }
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

  const handleSearchKeyPress = (e) => {
    if (e.key === "Enter") {
      performSearch();
    }
  };

  const goBackToSearch = () => {
    setView("search");
    setPaperData(null);
    setAnalyticsData(null);
    setError(null);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-50 via-white to-purple-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-indigo-600 via-purple-600 to-pink-600 shadow-xl sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-3">
              <Sparkles className="h-8 w-8 text-yellow-300" />
              <span className="text-xl font-bold text-white">
                Academic Research Hub
              </span>
            </div>
            <div className="flex items-center space-x-4">
              {view === "details" && (
                <button
                  onClick={goBackToSearch}
                  className="flex items-center text-white hover:text-yellow-300 transition-colors duration-200 px-4 py-2 rounded-md hover:bg-white/10"
                >
                  <ArrowLeft className="h-5 w-5 mr-2" />
                  Back to Search
                </button>
              )}
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
        {view === "search" ? (
          <>
            {/* Search Section */}
            <div className="mb-8">
              <div className="max-w-4xl mx-auto">
                <div className="text-center mb-8">
                  <h1 className="text-4xl font-bold text-slate-800 mb-3">
                    Discover Academic Papers
                  </h1>
                  <p className="text-lg text-slate-600">
                    Search millions of scientific papers using AI-powered
                    semantic search
                  </p>
                </div>

                {/* Search Bar */}
                <div className="relative mb-4">
                  <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 h-6 w-6 text-indigo-400" />
                  <input
                    type="text"
                    placeholder="Enter your research query (e.g., 'Machine learning in healthcare')"
                    className="w-full pl-14 pr-36 py-5 text-lg border-2 border-indigo-200 rounded-2xl focus:ring-4 focus:ring-indigo-100 focus:border-indigo-500 transition-all duration-200 shadow-lg bg-white"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    onKeyPress={handleSearchKeyPress}
                  />
                  <button
                    onClick={performSearch}
                    disabled={isSearching || !searchQuery.trim()}
                    className="absolute right-2 top-1/2 transform -translate-y-1/2 bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-8 py-3 rounded-xl hover:from-indigo-700 hover:to-purple-700 disabled:opacity-50 disabled:cursor-not-allowed font-semibold shadow-md hover:shadow-xl transition-all duration-200 flex items-center"
                  >
                    {isSearching ? (
                      <>
                        <Loader2 className="animate-spin h-5 w-5 mr-2" />
                        Searching...
                      </>
                    ) : (
                      <>
                        <Search className="h-5 w-5 mr-2" />
                        Search
                      </>
                    )}
                  </button>
                </div>

                {/* Advanced Search Options */}
                <div className="text-center">
                  <button
                    onClick={() => setShowAdvancedSearch(!showAdvancedSearch)}
                    className="text-indigo-600 hover:text-indigo-800 font-medium text-sm flex items-center mx-auto"
                  >
                    {showAdvancedSearch ? "Hide" : "Show"} Advanced Options
                    {showAdvancedSearch ? (
                      <ChevronUp className="ml-1 h-4 w-4" />
                    ) : (
                      <ChevronDown className="ml-1 h-4 w-4" />
                    )}
                  </button>
                </div>

                {showAdvancedSearch && (
                  <div className="mt-4 bg-white rounded-xl shadow-md p-6 border border-indigo-100">
                    <div className="grid grid-cols-2 gap-6">
                      <div>
                        <label className="block text-sm font-semibold text-slate-700 mb-2">
                          Number of Results (top_k)
                        </label>
                        <div className="w-full px-4 py-2 border-2 border-slate-200 rounded-lg bg-slate-50 text-slate-700 font-mono">
                          1000
                        </div>
                        <p className="text-xs text-slate-500 mt-1">
                          Fixed to 1000 results (duplicates removed
                          automatically)
                        </p>
                      </div>
                      <div>
                        <label className="block text-sm font-semibold text-slate-700 mb-2">
                          Search Precision (nprobe)
                        </label>
                        <input
                          type="number"
                          min="1"
                          max="2048"
                          value={nprobe}
                          onChange={(e) => setNprobe(parseInt(e.target.value))}
                          className="w-full px-4 py-2 border-2 border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-200 focus:border-indigo-500"
                        />
                      </div>
                    </div>
                  </div>
                )}

                {/* Example Queries */}
                <div className="mt-6 text-center text-sm text-slate-600">
                  <span className="font-medium">Try: </span>
                  {[
                    "Machine learning in healthcare",
                    "Climate change mitigation",
                    "Quantum computing applications",
                  ].map((example, idx) => (
                    <button
                      key={idx}
                      onClick={() => setSearchQuery(example)}
                      className="text-indigo-600 hover:text-indigo-800 font-medium mx-2 hover:underline"
                    >
                      {example}
                    </button>
                  ))}
                </div>
              </div>

              {/* Error Message */}
              {error && (
                <div className="max-w-4xl mx-auto mt-6 p-4 bg-red-50 border-2 border-red-200 rounded-xl flex items-start shadow-md">
                  <AlertCircle className="h-5 w-5 text-red-500 mr-3 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-red-800 font-semibold">Error</p>
                    <p className="text-red-700 text-sm mt-1">{error}</p>
                  </div>
                </div>
              )}
            </div>

            {/* Search Results */}
            {(searchResults.length > 0 || enrichedResults.length > 0) && (
              <div className="max-w-6xl mx-auto">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-2xl font-bold text-slate-800 flex items-center">
                    <TrendingUp className="h-6 w-6 mr-2 text-indigo-600" />
                    Search Results (
                    {enrichedResults.length || searchResults.length} unique
                    papers)
                    {isEnriching && " - Enriching with impact metrics..."}
                  </h2>
                </div>

                {isEnriching && (
                  <div className="mb-4 p-4 bg-indigo-50 border-2 border-indigo-200 rounded-xl flex items-center">
                    <Loader2 className="animate-spin h-5 w-5 text-indigo-600 mr-3" />
                    <div>
                      <p className="text-indigo-800 font-semibold">
                        Fetching Impact Metrics
                      </p>
                      <p className="text-indigo-600 text-sm">
                        Retrieving FWCI and citation percentiles for all
                        papers...
                      </p>
                    </div>
                  </div>
                )}

                {/* Word Cloud */}
                <WordCloud
                  titles={(enrichedResults.length > 0
                    ? enrichedResults
                    : searchResults
                  ).map((r) => r.title)}
                />

                <div className="space-y-4">
                  {(enrichedResults.length > 0
                    ? enrichedResults
                    : searchResults
                  ).map((result, index) => (
                    <SearchResultCard
                      key={`${result.corpus_id}-${index}`}
                      result={result}
                      onViewDetails={fetchPaperDetails}
                      enrichment={result.enrichment}
                    />
                  ))}
                </div>
              </div>
            )}

            {/* Empty State */}
            {!isSearching &&
              !isEnriching &&
              searchResults.length === 0 &&
              enrichedResults.length === 0 &&
              !error && (
                <div className="max-w-4xl mx-auto mt-16">
                  <div className="bg-white rounded-2xl shadow-xl border-2 border-slate-100 p-16">
                    <div className="flex flex-col items-center justify-center text-center">
                      <div className="bg-gradient-to-br from-indigo-100 to-purple-100 rounded-full p-6 mb-6">
                        <BookOpen className="h-16 w-16 text-indigo-600" />
                      </div>
                      <h3 className="text-2xl font-bold text-slate-800 mb-3">
                        Ready to Explore
                      </h3>
                      <p className="text-slate-600 text-lg max-w-md">
                        Enter a search query above to discover relevant academic
                        papers using our advanced semantic search engine
                      </p>
                    </div>
                  </div>
                </div>
              )}
          </>
        ) : (
          <>
            {/* Paper Details View */}
            {isFetchingPaper && (
              <div className="max-w-5xl mx-auto bg-white rounded-2xl shadow-xl border-2 border-indigo-100 p-16">
                <div className="flex flex-col items-center justify-center">
                  <Loader2 className="animate-spin h-20 w-20 text-indigo-600 mb-6" />
                  <p className="text-slate-700 font-semibold text-xl">
                    Fetching paper details and analytics...
                  </p>
                  <p className="text-slate-500 text-sm mt-2">
                    This may take up to 2 minutes
                  </p>
                </div>
              </div>
            )}

            {error && !isFetchingPaper && (
              <div className="max-w-5xl mx-auto p-4 bg-red-50 border-2 border-red-200 rounded-xl flex items-start shadow-md">
                <AlertCircle className="h-5 w-5 text-red-500 mr-3 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="text-red-800 font-semibold">Error</p>
                  <p className="text-red-700 text-sm mt-1">{error}</p>
                </div>
              </div>
            )}

            {paperData && !isFetchingPaper && (
              <div className="max-w-6xl mx-auto">
                <PaperDetailCard
                  paper={paperData}
                  analytics={analyticsData}
                  isSaved={savedPapers.has(paperData.corpusid)}
                  onToggleSave={() => toggleSavedPaper(paperData.corpusid)}
                  showAdvancedInfo={showAdvancedInfo}
                  setShowAdvancedInfo={setShowAdvancedInfo}
                  showFullAnalytics={showFullAnalytics}
                  setShowFullAnalytics={setShowFullAnalytics}
                />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default App;
