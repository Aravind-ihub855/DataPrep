import React, { useState } from "react";

const UploadPage = () => {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState("");
  const [details, setDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [expandedSection, setExpandedSection] = useState(null);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setMessage("");
    setDetails(null);
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage("Please select a file");
      return;
    }

    setLoading(true);
    setMessage("");
    setDetails(null);

    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await fetch("http://localhost:8000/upload-with-details/", {
        method: "POST",
        body: formData,
      });
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      const data = await res.json();
      setDetails(data);
      setMessage(data.message);
    } catch (err) {
      setMessage("Upload failed: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const toggleSection = (section) => {
    setExpandedSection(expandedSection === section ? null : section);
  };

  return (
    <div className="min-h-screen bg-gray-100 p-6 md:p-8">
      <div className="max-w-4xl mx-auto bg-white rounded-lg shadow-lg p-6">
        <h1 className="text-3xl font-bold text-gray-800 mb-6">Upload CSV</h1>
        
        {/* File Input and Upload Button */}
        <div className="flex flex-col md:flex-row gap-4 mb-6">
          <input
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            className="flex-1 border border-gray-300 rounded-md p-2 text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            onClick={handleUpload}
            disabled={loading}
            className={`bg-blue-500 text-white px-6 py-2 rounded-md hover:bg-blue-600 transition disabled:bg-blue-300 ${loading ? 'cursor-not-allowed' : ''}`}
          >
            {loading ? (
              <span className="flex items-center">
                <svg className="animate-spin h-5 w-5 mr-2" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8h8a8 8 0 01-8 8 8 8 0 01-8-8z" />
                </svg>
                Uploading...
              </span>
            ) : (
              "Upload"
            )}
          </button>
        </div>

        {/* Message Display */}
        {message && (
          <p
            className={`text-sm mb-6 ${
              message.includes("failed")
                ? "text-red-500"
                : message.includes("No new rows inserted")
                ? "text-yellow-500"
                : "text-green-500"
            }`}
          >
            {message}
          </p>
        )}

        {/* Details Display */}
        {details && (
          <div className="space-y-4">
            {/* Inferred Data Types */}
            <div className="border rounded-md">
              <button
                onClick={() => toggleSection("inferred_types")}
                className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
              >
                Inferred Data Types
              </button>
              {expandedSection === "inferred_types" && (
                <div className="p-4 bg-gray-50">
                  <ul className="list-disc list-inside text-gray-700">
                    {details.inferred_types.map(([col, type], index) => (
                      <li key={index}>
                        <span className="font-medium">{col}</span>: {type}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            {/* Sample Row Numbers */}
            <div className="border rounded-md">
              <button
                onClick={() => toggleSection("sample_rows")}
                className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
              >
                Sample Row Numbers
              </button>
              {expandedSection === "sample_rows" && (
                <div className="p-4 bg-gray-50">
                  <p className="text-gray-700">
                    Rows selected: {details.sample_row_numbers.join(", ") || "None"}
                  </p>
                </div>
              )}
            </div>

            {/* Generated Schema */}
            <div className="border rounded-md">
              <button
                onClick={() => toggleSection("schema")}
                className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
              >
                Generated Schema
              </button>
              {expandedSection === "schema" && (
                <div className="p-4 bg-gray-50">
                  <pre className="text-sm text-gray-700 whitespace-pre-wrap">
                    {details.schema_query || "Schema matched with existing table, no new schema generated."}
                  </pre>
                </div>
              )}
            </div>

            {/* Inserted Rows */}
            <div className="border rounded-md">
              <button
                onClick={() => toggleSection("inserted_rows")}
                className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
              >
                Inserted Rows (Sample)
              </button>
              {expandedSection === "inserted_rows" && (
                <div className="p-4 bg-gray-50 overflow-x-auto">
                  <table className="min-w-full text-sm text-gray-700">
                    <thead>
                      <tr className="bg-gray-100">
                        {details.inserted_rows.length > 0 &&
                          Object.keys(details.inserted_rows[0]).map((key) => (
                            <th key={key} className="p-2 border-b">{key}</th>
                          ))}
                      </tr>
                    </thead>
                    <tbody>
                      {details.inserted_rows.map((row, index) => (
                        <tr key={index} className="border-b">
                          {Object.values(row).map((value, i) => (
                            <td key={i} className="p-2">{value}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Metadata Information */}
            <div className="border rounded-md">
              <button
                onClick={() => toggleSection("metadata")}
                className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
              >
                Metadata Information
              </button>
              {expandedSection === "metadata" && (
                <div className="p-4 bg-gray-50">
                  <ul className="list-disc list-inside text-gray-700">
                    {Object.entries(details.metadata).map(([key, value]) => (
                      <li key={key}>
                        <span className="font-medium">{key}</span>: {value}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default UploadPage;
