const { execSync } = require('child_process');
const fs = require('fs');
const yaml = require('js-yaml');
const path = require('path');

// Enhanced SQL Parser - handles multiple SELECT statements and complex structures
function extractColumnsFromSQL(content) {
  if (!content || typeof content !== 'string') return [];
  
  // Remove comments to avoid parsing issues
  const cleanContent = content
    .replace(/--.*$/gm, '') // Remove single-line comments
    .replace(/\/\*[\s\S]*?\*\//g, ''); // Remove multi-line comments
  
  // Find all SELECT statements (including CTEs and subqueries)
  const selectRegex = /SELECT\s+([\s\S]+?)\s+FROM/gi;
  const allSelects = [];
  let match;
  
  // Extract all SELECT statements
  while ((match = selectRegex.exec(cleanContent)) !== null) {
    allSelects.push(match[1]);
  }
  
  // If no SELECT statements found, try alternative patterns
  if (allSelects.length === 0) {
    // Look for final SELECT without FROM (for some DBT patterns)
    const finalSelectRegex = /SELECT\s+([\s\S]+?)(?:\s+FROM|\s*$)/gi;
    const finalMatch = finalSelectRegex.exec(cleanContent);
    if (finalMatch) {
      allSelects.push(finalMatch[1]);
    }
  }
  
  // Process the last SELECT statement (usually the final output)
  const finalSelect = allSelects[allSelects.length - 1];
  if (!finalSelect) return [];
  
  // Enhanced column extraction with better handling of complex expressions
  const columns = finalSelect
    .split(/\s*,\s*(?![^(]*\))/) // Split by comma, but not inside parentheses
    .map(col => {
      // Handle various column patterns
      let cleaned = col
        .replace(/\s+as\s+.*$/i, '') // Remove AS aliases
        .replace(/.*\./g, '') // Remove table prefixes
        .trim()
        .split(/\s+/)[0] // Take first word
        .replace(/[`"']/g, '') // Remove quotes
        .replace(/\(.*$/, ''); // Remove function calls
      
      // Handle special cases
      if (cleaned.includes('(') && cleaned.includes(')')) {
        // For function calls, extract the function name
        cleaned = cleaned.split('(')[0];
      }
      
      return cleaned;
    })
    .filter(col => {
      // Filter out invalid columns
      return col && 
             !col.startsWith('--') && 
             !col.match(/^\d+$/) && // Not just numbers
             col.length > 0 &&
             !col.match(/^(and|or|where|group|order|having)$/i); // Not SQL keywords
    });
  
  return [...new Set(columns)]; // Remove duplicates
}

// Enhanced YML Parser with specific support for your format
function extractColumnsFromYML(content, filePath) {
  try {
    const schema = yaml.load(content);
    if (!schema) return [];

    // Case 1: Standard DBT format (models array)
    if (Array.isArray(schema.models)) {
      // Extract ALL columns from ALL models (if multiple exist)
      return schema.models.flatMap(model => 
        model.columns?.map(col => 
          typeof col === 'string' ? { name: col } : col
        ) || []
      );
    }

    // Case 2: Direct columns definition (fallback)
    if (schema.columns) {
      return schema.columns.map(col => 
        typeof col === 'string' ? { name: col } : col
      );
    }

    return []; // No columns found
  } catch (e) {
    console.error(`YML parsing error:`, e);
    return [];
  }
}

// Enhanced Git Helper with better error handling
function getFileContent(sha, filePath) {
  try {
    return execSync(`git show ${sha}:${filePath}`, { 
      stdio: ['pipe', 'pipe', 'ignore'],
      encoding: 'utf-8',
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer for large files
    });
  } catch (error) {
    if (error.message.includes('exists on disk, but not in')) {
      console.log(`File not found in ${sha}: ${filePath}`);
    } else {
      console.error(`Error reading ${filePath}:`, error.message);
    }
    return null;
  }
}

module.exports = {
  extractColumnsFromSQL,
  extractColumnsFromYML,
  getFileContent
};