#!/bin/bash

testThreads=32
bufferSize=1000

failures=0

if make mr-wordc
then
  inputDir="input/mr-wordc"
  outputDir="output/mr-wordc"
  compareDir="output_compare/mr-wordc"

  inputFiles=($(find "${inputDir}/" -type f -printf "%f\n" | sort | tr '\n' ' '))

  mkdir -p "${outputDir}"
  mkdir -p "${outputDir}/diffs"
  mkdir -p "${outputDir}/out"

  for file in "${inputFiles[@]}"
  do
    inputFile="./${inputDir}/${file}"
    outputFile="./${outputDir}/${file}"
    outputCompareFile="./${compareDir}/${file}"
    outFile="./${outputDir}/out/${file}"
    diffFile="./${outputDir}/diffs/${file}"
    { ./mr-wordc "${inputFile}" "${outputFile}" "${testThreads}" "${bufferSize}" ; } &> "${outFile}"
    if diff "${outputFile}" "${outputCompareFile}" > "${diffFile}"
    then
      echo "Test Case mr-wordc ${type} ${inputFile} passed"
    else
      echo "Test Case mr-wordc ${type} ${inputFile} failed"
      echo "  Incorrect output for ${outputFile}. Check diff file."
      let failures++
    fi
  done

  if [ "${failures}" -eq 0 ]
  then
    echo "All tests passed"
  else
    echo "Failures: ${failures}"
  fi

else
  echo "Make failed"
fi
