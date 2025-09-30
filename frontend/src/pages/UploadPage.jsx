import React, { useState, useEffect } from "react";
import { Folder, File, Trash2 } from "lucide-react";

const UploadPage = () => {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState("");
  const [details, setDetails] = useState({
    inferred_types: [],
    sample_row_numbers: [],
    schema_query: null,
    inserted_rows: [],
    metadata: {},
    duplicates: [],
    has_duplicates: false,
  });
  const [loading, setLoading] = useState(false);
  const [expandedSection, setExpandedSection] = useState(null);
  const [batchFileIds, setBatchFileIds] = useState([]);
  const [selectedBatchId, setSelectedBatchId] = useState(null);
  const [selectedFileId, setSelectedFileId] = useState(null);
  const [batchData, setBatchData] = useState(null);
  const [showConfirmDialog, setShowConfirmDialog] = useState(false);
  const [step, setStep] = useState(0);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setMessage("");
    setDetails({
      inferred_types: [],
      sample_row_numbers: [],
      schema_query: null,
      inserted_rows: [],
      metadata: {},
      duplicates: [],
      has_duplicates: false,
    });
    setStep(0);
    setShowConfirmDialog(false);
    setBatchData(null); // Clear batch preview
    setSelectedBatchId(null); // Clear selected batch
    setSelectedFileId(null); // Clear selected file
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage("Please select a file");
      return;
    }

    setLoading(true);
    setMessage("");

    try {
      setMessage("Inferring data types...");
      const formData1 = new FormData();
      formData1.append("file", file);
      const res1 = await fetch("http://localhost:8000/infer-types/", {
        method: "POST",
        body: formData1,
      });
      if (!res1.ok) throw new Error(`HTTP error! status: ${res1.status}`);
      const data1 = await res1.json();
      setDetails((prev) => ({ ...prev, inferred_types: data1.inferred_types }));
      setStep(1);

      setMessage("Sampling rows...");
      const formData2 = new FormData();
      formData2.append("file", file);
      const res2 = await fetch("http://localhost:8000/sample-rows/", {
        method: "POST",
        body: formData2,
      });
      if (!res2.ok) throw new Error(`HTTP error! status: ${res2.status}`);
      const data2 = await res2.json();
      setDetails((prev) => ({ ...prev, sample_row_numbers: data2.sample_row_numbers }));
      setStep(2);

      setMessage("Generating schema...");
      const formData3 = new FormData();
      formData3.append("file", file);
      const res3 = await fetch("http://localhost:8000/generate-schema/", {
        method: "POST",
        body: formData3,
      });
      if (!res1.ok) throw new Error(`HTTP error! status: ${res3.status}`);
      const data3 = await res3.json();
      setDetails((prev) => ({ ...prev, schema_query: data3.schema_query, target_table: data3.target_table }));
      setStep(3);

      setMessage("Checking for duplicates...");
      const formData4 = new FormData();
      formData4.append("file", file);
      const res4 = await fetch("http://localhost:8000/check-duplicates/", {
        method: "POST",
        body: formData4,
      });
      if (!res4.ok) throw new Error(`HTTP error! status: ${res4.status}`);
      const data4 = await res4.json();
      setDetails((prev) => ({ ...prev, duplicates: data4.duplicates, has_duplicates: data4.has_duplicates }));
      setMessage(data4.message);
      
      if (data4.has_duplicates) {
        setShowConfirmDialog(true);
        setStep(4);
      } else {
        await proceedWithInsert(true);
      }
    } catch (err) {
      setMessage("Upload failed: " + err.message);
      setStep(0);
    } finally {
      setLoading(false);
    }
  };

  const proceedWithInsert = async (proceed) => {
    try {
      setMessage("Inserting rows...");
      const formData5 = new FormData();
      formData5.append("file", file);
      const res5 = await fetch(`http://localhost:8000/confirm-insert/?proceed=${proceed}`, {
        method: "POST",
        body: formData5,
      });
      if (!res5.ok) throw new Error(`HTTP error! status: ${res5.status}`);
      const data5 = await res5.json();
      setDetails((prev) => ({
        ...prev,
        inserted_rows: data5.row_count > 0 ? prev.inserted_rows : [],
        metadata: data5.row_count > 0 ? {
          table_name: data5.table_name,
          file_id: data5.file_id,
          batch_id: data5.batch_id,
          run_id: data5.run_id,
          row_count: data5.row_count,
        } : {},
      }));
      setMessage(data5.message);
      setStep(5);
      setShowConfirmDialog(false);
      fetchBatchFileIds(); // Refresh batch/file IDs after insert
    } catch (err) {
      setMessage("Upload failed: " + err.message);
      setStep(0);
    } finally {
      setLoading(false);
    }
  };

  const fetchBatchFileIds = async () => {
    try {
      const res = await fetch("http://localhost:8000/get-batch-file-ids/");
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setBatchFileIds(data.batch_file_ids);
    } catch (err) {
      setMessage("Failed to fetch batch and file IDs: " + err.message);
    }
  };

  const handlePreviewBatch = async (batchId, fileId) => {
    try {
      const res = await fetch(`http://localhost:8000/preview-batch-data/${batchId}?file_id=${fileId}`);
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setBatchData(data);
      setSelectedBatchId(batchId);
      setSelectedFileId(fileId);
    } catch (err) {
      setMessage("Failed to preview batch data: " + err.message);
    }
  };

  const handleDeleteBatch = async (batchId) => {
    if (window.confirm(`Are you sure you want to delete data for batch_id ${batchId}?`)) {
      try {
        const res = await fetch(`http://localhost:8000/delete-batch-data/${batchId}`, {
          method: "DELETE",
        });
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        const data = await res.json();
        setMessage(data.message);
        setBatchData(null);
        setSelectedBatchId(null);
        setSelectedFileId(null);
        fetchBatchFileIds();
      } catch (err) {
        setMessage("Failed to delete batch data: " + err.message);
      }
    }
  };

  useEffect(() => {
    fetchBatchFileIds();
  }, []);

  const toggleSection = (section) => {
    setExpandedSection(expandedSection === section ? null : section);
  };

  const organizedData = batchFileIds.reduce((acc, { file_id, batch_id }) => {
    if (!acc[file_id]) {
      acc[file_id] = [];
    }
    acc[file_id].push(batch_id);
    return acc;
  }, {});

  return (
    <div className="min-h-screen bg-gray-50 flex">
      {/* Sidebar */}
      <div className="w-80 bg-white shadow-xl p-6 flex-shrink-0">
        <h2 className="text-2xl font-bold text-gray-900 mb-6">Data Hierarchy</h2>
        <div className="space-y-4">
          {Object.entries(organizedData).map(([fileId, batchIds]) => (
            <div key={fileId} className="border-l-2 border-gray-200 pl-4">
              <div className="flex items-center gap-2">
                <Folder className="w-5 h-5 text-blue-500" />
                <span className="font-semibold text-gray-800">File ID: {fileId}</span>
              </div>
              <ul className="ml-6 mt-2 space-y-2">
                {batchIds.map((batchId) => (
                  <li key={batchId} className="flex items-center justify-between">
                    <button
                      onClick={() => handlePreviewBatch(batchId, fileId)}
                      className={`flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 ${
                        selectedBatchId === batchId && selectedFileId === fileId ? "font-bold" : ""
                      }`}
                    >
                      <File className="w-4 h-4" />
                      Batch ID: {batchId}
                    </button>
                    <button
                      onClick={() => handleDeleteBatch(batchId)}
                      className="text-red-500 hover:text-red-700"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 p-8 flex flex-col items-center">
        <div className="w-full max-w-4xl bg-white rounded-xl shadow-lg p-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-8">CSV Data Uploader</h1>

          {/* File Input and Upload Button */}
          <div className="flex flex-col sm:flex-row gap-4 mb-8">
            <input
              type="file"
              accept=".csv"
              onChange={handleFileChange}
              className="flex-1 border border-gray-300 rounded-lg p-3 text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition"
            />
            <button
              onClick={handleUpload}
              disabled={loading}
              className={`flex items-center justify-center px-6 py-3 rounded-lg text-white font-medium transition ${
                loading ? "bg-blue-400 cursor-not-allowed" : "bg-blue-600 hover:bg-blue-700"
              }`}
            >
              {loading ? (
                <span className="flex items-center gap-2">
                  <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8h8a8 8 0 01-8 8 8 8 0 01-8-8z" />
                  </svg>
                  Processing...
                </span>
              ) : (
                "Upload CSV"
              )}
            </button>
          </div>

          {/* Message Display */}
          {message && (
            <p
              className={`text-sm mb-6 p-3 rounded-lg ${
                message.includes("failed")
                  ? "bg-red-100 text-red-700"
                  : message.includes("No new rows inserted") || message.includes("canceled")
                  ? "bg-yellow-100 text-yellow-700"
                  : "bg-green-100 text-green-700"
              }`}
            >
              {message}
            </p>
          )}

          {/* Data Preview */}
          {batchData && (
            <div className="mb-8 w-full">
              <h3 className="text-xl font-semibold text-gray-800 mb-4">Batch {selectedBatchId} (File {selectedFileId}) Preview</h3>
              <div className="bg-gray-50 rounded-lg p-4 max-h-96 overflow-y-auto shadow-sm">
                <table className="w-full text-sm text-gray-700">
                  <thead>
                    <tr className="bg-gray-100 sticky top-0">
                      {batchData.rows.length > 0 &&
                        Object.keys(batchData.rows[0]).map((key) => (
                          <th key={key} className="p-3 text-left font-semibold border-b">{key}</th>
                        ))}
                    </tr>
                  </thead>
                  <tbody>
                    {batchData.rows.map((row, index) => (
                      <tr key={index} className="border-b hover:bg-gray-100">
                        {Object.values(row).map((value, i) => (
                          <td key={i} className="p-3">{value}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Details Display */}
          {details && (
            <div className="space-y-6">
              {step >= 1 && (
                <div className="border rounded-lg shadow-sm">
                  <button
                    onClick={() => toggleSection("inferred_types")}
                    className="w-full text-left bg-gray-100 p-4 font-semibold text-gray-800 rounded-t-lg hover:bg-gray-200 transition"
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
              )}

              {step >= 2 && (
                <div className="border rounded-lg shadow-sm">
                  <button
                    onClick={() => toggleSection("sample_rows")}
                    className="w-full text-left bg-gray-100 p-4 font-semibold text-gray-800 rounded-t-lg hover:bg-gray-200 transition"
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
              )}

              {step >= 3 && (
                <div className="border rounded-lg shadow-sm">
                  <button
                    onClick={() => toggleSection("schema")}
                    className="w-full text-left bg-gray-100 p-4 font-semibold text-gray-800 rounded-t-lg hover:bg-gray-200 transition"
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
              )}

              {step >= 5 && details.inserted_rows.length > 0 && (
                <div className="border rounded-lg shadow-sm">
                  <button
                    onClick={() => toggleSection("inserted_rows")}
                    className="w-full text-left bg-gray-100 p-4 font-semibold text-gray-800 rounded-t-lg hover:bg-gray-200 transition"
                  >
                    Inserted Rows (Sample)
                  </button>
                  {expandedSection === "inserted_rows" && (
                    <div className="p-4 bg-gray-50 overflow-x-auto">
                      <table className="w-full text-sm text-gray-700">
                        <thead>
                          <tr className="bg-gray-100">
                            {details.inserted_rows.length > 0 &&
                              Object.keys(details.inserted_rows[0]).map((key) => (
                                <th key={key} className="p-3 text-left font-semibold border-b">{key}</th>
                              ))}
                          </tr>
                        </thead>
                        <tbody>
                          {details.inserted_rows.map((row, index) => (
                            <tr key={index} className="border-b hover:bg-gray-100">
                              {Object.values(row).map((value, i) => (
                                <td key={i} className="p-3">{value}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}

              {step >= 5 && Object.keys(details.metadata).length > 0 && (
                <div className="border rounded-lg shadow-sm">
                  <button
                    onClick={() => toggleSection("metadata")}
                    className="w-full text-left bg-gray-100 p-4 font-semibold text-gray-800 rounded-t-lg hover:bg-gray-200 transition"
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
              )}
            </div>
          )}
        </div>
      </div>

      {/* Confirmation Dialog for Duplicates */}
      {showConfirmDialog && details.has_duplicates && (
        <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-8 max-w-3xl w-full shadow-2xl">
            <h2 className="text-2xl font-bold text-gray-900 mb-6">{details.message}</h2>
            <p className="text-gray-700 mb-4">Preview of duplicate rows (up to 5):</p>
            <div className="bg-gray-50 rounded-lg p-4 mb-6 overflow-x-auto shadow-sm">
              <table className="w-full text-sm text-gray-700">
                <thead>
                  <tr className="bg-gray-100">
                    {details.duplicates.length > 0 &&
                      Object.keys(details.duplicates[0]).map((key) => (
                        <th key={key} className="p-3 text-left font-semibold border-b">{key}</th>
                      ))}
                  </tr>
                </thead>
                <tbody>
                  {details.duplicates.map((row, index) => (
                    <tr key={index} className="border-b hover:bg-gray-100">
                      {Object.values(row).map((value, i) => (
                        <td key={i} className="p-3">{value}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-gray-700 mb-6">Proceed with inserting unique rows?</p>
            <div className="flex gap-4 justify-end">
              <button
                onClick={() => proceedWithInsert(true)}
                className="bg-green-600 text-white px-6 py-2 rounded-lg hover:bg-green-700 transition"
              >
                Yes
              </button>
              <button
                onClick={() => proceedWithInsert(false)}
                className="bg-red-600 text-white px-6 py-2 rounded-lg hover:bg-red-700 transition"
              >
                No
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadPage;