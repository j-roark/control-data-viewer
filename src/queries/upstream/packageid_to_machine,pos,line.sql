SELECT threadlinesubject.MachineName, threadlinesubject.PositionName, threadlinesubject.ThreadlineName FROM package
LEFT JOIN threadlinesubject
ON threadlinesubject.ThreadlineID = package.ThreadlineID
WHERE PackageID = /* package id */
GROUP BY package.ThreadlineID;